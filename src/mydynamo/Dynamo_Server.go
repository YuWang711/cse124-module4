package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"errors"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	Dynamo_Store   map[string]*DynamoResult //Key value store
	clock	       VectorClock
	m	       sync.Mutex
	Crashed	       bool
	CalledFrom     map[string]bool
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	log.Print("Nodie ID :", s.nodeID)
	log.Print("Gossip")
	if s.Crashed == true {
		return errors.New("Node Crashed")
	}
	for _,pref := range s.preferenceList {
		for index,element := range s.Dynamo_Store{
			for _, object := range element.EntryList{
				var result bool
				var new_PutArgs PutArgs
				new_PutArgs.Context = object.Context
				new_PutArgs.Value = object.Value
				new_PutArgs.Key = index
				address := pref.Address + ":" + pref.Port
				rpc_call,e := rpc.DialHTTP("tcp", address)
				if e != nil {
					log.Print(e)
					//Server Crashed
					break
				}
				e = rpc_call.Call("MyDynamo.Put", new_PutArgs, &result)
				if e != nil {
					log.Print(e)
				}
			}

		}
	}
	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	s.m.Lock()
	s.Crashed = true
	time.Sleep( time.Duration(seconds) * time.Second)
	s.Crashed = false
	s.m.Unlock()
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	//Attemp to put value into its local key/value store.
	log.Print("Nodie ID :", s.nodeID)
	log.Print("In Put")
	if s.Crashed == true {
		return errors.New("Node Crashed")
	}
	log.Print(value.Value)
	log.Print("In Put, Before checking new < old ")
	if _,ok := s.Dynamo_Store[value.Key]; ok {
		for _,element := range s.Dynamo_Store[value.Key].EntryList {
			if value.Context.Clock.LessThan(element.Context.Clock){
				if value.Context.Clock.Concurrent(element.Context.Clock) {
					if value.Context.Clock.Equals(element.Context.Clock) {
						log.Print(value.Context.Clock)
						log.Print(element.Context.Clock)
						return errors.New("Put has failed new Context < old Context")
					}
					continue
				}
				log.Print(value.Context.Clock)
				log.Print(element.Context.Clock)
				return errors.New("Put has failed new Context < old Context")
			}
		}
		log.Print("In Put, After checking new < old ")
		for _,element := range s.Dynamo_Store[value.Key].EntryList {
			if element.Context.Clock.LessThan(value.Context.Clock){
				if element.Context.Clock.Concurrent(value.Context.Clock) {
					continue
				}
				log.Print("In Put, old < new ")
				s.m.Lock()
				s.clock.Increment(s.nodeID)
				s.m.Unlock()
				//PUTTING INTO LOCAL
				//Case when there is at least one clock in current key that is less than
				//New clock

				s.m.Lock()
				var new_EntryList []ObjectEntry
				var new_Object ObjectEntry
				new_Object.Context = value.Context
				new_Object.Value = value.Value
				new_EntryList = append(new_EntryList, new_Object)
				s.Dynamo_Store[value.Key].EntryList = new_EntryList
				s.m.Unlock()

				var new_result bool
				new_result = false
				i := 0
				var Wvalue = s.wValue
				for i < (Wvalue) {
					log.Print("Send to others")
					log.Print(len(s.preferenceList))
					if i >= len(s.preferenceList) {
						break
					}
					var pref = s.preferenceList[i]
					address := pref.Address + ":" + pref.Port
					log.Print("Send to address: ", address)
					if _,ok := s.CalledFrom[address]; !ok{
						s.CalledFrom[address] = true
					} else {
						i++
						continue
					}
					if s.preferenceList[i] == s.selfNode{
						i++
						Wvalue++
						log.Print("Skip")
						continue
					}
					rpc_call,e := rpc.DialHTTP("tcp", address)
					if e != nil {
						log.Print(e)
					}
					log.Print("Calling Put")
					e = rpc_call.Call("MyDynamo.Put", value, &new_result)
					if e != nil {
						log.Print(e)
					}
					if new_result == false {
						i++
						Wvalue++
						continue
					}
					log.Print("Finish send to others")
					i++
				}
				for index,_ := range s.CalledFrom {
					s.CalledFrom[index] = false
				}
				*result = true
				return nil
			}
		}
		log.Print("In Put, Case New == Old")
		s.m.Lock()
		var new_Object ObjectEntry
		new_Object.Context = value.Context
		new_Object.Value = value.Value
		log.Print("nodeId", s.nodeID)
		for i,_ := range s.Dynamo_Store[value.Key].EntryList {
			for index,element := range s.Dynamo_Store[value.Key].EntryList[i].Value{
				if element != new_Object.Value[index] {
					s.Dynamo_Store[value.Key].EntryList = append(s.Dynamo_Store[value.Key].EntryList,new_Object)
					break
				}
			}
		}
		s.m.Unlock()
		*result = true
		log.Print("Out Put")
		return nil
	} else {
		s.m.Lock()
		s.clock.Increment(s.nodeID)
		s.m.Unlock()
		log.Print("New Key found")
		s.m.Lock()
		var new_DynamoResult DynamoResult
		var new_EntryList []ObjectEntry
		var new_Object ObjectEntry
		new_Object.Context = value.Context
		new_Object.Value = value.Value
		new_EntryList = append(new_EntryList, new_Object)
		new_DynamoResult.EntryList = new_EntryList
		s.Dynamo_Store[value.Key] = &new_DynamoResult
		log.Print("Finished creating new key")
		s.m.Unlock()

		var new_result bool
		new_result = false
		i := 0
		var Wvalue = s.wValue
		for i < (Wvalue) {
			log.Print("Send to others")
			if i >= len(s.preferenceList) {
				break
			}
			var pref = s.preferenceList[i]
			address := pref.Address + ":" + pref.Port
			log.Print("Send to address: ", address)
			if _,ok := s.CalledFrom[address]; !ok{
				s.CalledFrom[address] = true
			} else {
				i++
				continue
			}
			if s.preferenceList[i] == s.selfNode{
				i++
				Wvalue++
				continue
			}
			rpc_call,e := rpc.DialHTTP("tcp", address)
			if e != nil {
				log.Print(e)
			}
			e = rpc_call.Call("MyDynamo.Put", value, &new_result)
			if e != nil {
				log.Print(e)
			}
			if new_result == false {
				i++
				Wvalue++
				continue
			}
			log.Print("Finish send to others")
			i++
		}
		for index,_ := range s.CalledFrom {
			s.CalledFrom[index] = false
		}
		*result = true
		return nil
	}
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if s.Crashed == true {
		return errors.New("Node Crashed")
	}
	log.Print("In GET")
	log.Print(s.Dynamo_Store[key].EntryList)
	//Need to work with Dynamo Result here
	if _, ok := s.Dynamo_Store[key]; !ok{
		return errors.New("Node Key was not found")
	}
	log.Print("Key is found - Get")
	for _,element := range s.Dynamo_Store[key].EntryList {
		element.Context = NewContext(s.clock)
		result.EntryList = append(result.EntryList, element)
	}
	i := 0
	var Rvalue = s.rValue
	var temp_result DynamoResult
	var clocks = make([]VectorClock,0)
	log.Print("Sending to others")
	for i < (Rvalue) {
		if i >= len(s.preferenceList) {
			break
		}
		if s.preferenceList[i] == s.selfNode {
			i++
			continue
		}
		var new_DynamoResult DynamoResult
		var pref = s.preferenceList[i]
		address := pref.Address + ":" + pref.Port
		if _,ok := s.CalledFrom[address]; !ok{
			s.CalledFrom[address] = true
		} else {
			i++
			continue
		}
		rpc_call,e := rpc.DialHTTP("tcp", address)
		if e != nil {
			log.Print(e)
		}
		e = rpc_call.Call("MyDynamo.Get", key, &new_DynamoResult)
		if e != nil {
			i++
			Rvalue++
			log.Print(e)
			continue
		} else {
			for _,element := range new_DynamoResult.EntryList {
				temp_result.EntryList = append(temp_result.EntryList, element)
				clocks = append(clocks, element.Context.Clock)
			}
		}
		i++
	}
	for index,_ := range s.CalledFrom{
		s.CalledFrom[index] = false
	}
	log.Print("Finished sending")
	s.clock.Combine(clocks)
	for _,element := range temp_result.EntryList{
		if element.Context.Clock.Concurrent(s.clock) {
			result.EntryList = append(result.EntryList, element)
		}
	}
	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	var mutex sync.Mutex
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		Dynamo_Store:	make(map[string]*DynamoResult),
		clock:		NewVectorClock(),
		m:		mutex,
		Crashed:	false,
		CalledFrom:	make(map[string]bool),
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
