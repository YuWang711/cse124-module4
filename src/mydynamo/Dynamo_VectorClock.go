package mydynamo


type VectorClock struct {
	//todo
	NodeId_version map[string]uint64
}
/*
	Two Update rules:
	1. For each local event on process i, increment local entry c_i
	2. If process j receives message with vector [d_1,d_2, .... , d_n]:
		set each lcoal entry c_k = max{c_k,d_k}
		increment local entry c_j
*/


//Creates a new VectorClock
func NewVectorClock() VectorClock {
	return VectorClock{
		NodeId_version: make(map[string]uint64),
	}
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	if s.Equals(otherClock) == false {
		for index,element := range s.NodeId_version{
			if _, ok := otherClock.NodeId_version[index];ok{
				if element > otherClock.NodeId_version[index]{
					return false
				}
			} else {
				if element > 0 {
					return false
				}
			}
		}
	}
	return true
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	if s.LessThan(otherClock) == false{
		return false
	}
	if otherClock.LessThan(s) == false {
		return false
	}
	return true
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	s.NodeId_version[nodeId] += 1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	for _,clock := range clocks{
		for index, element := range clock.NodeId_version {
			if _, ok := s.NodeId_version[index];ok{
				s.NodeId_version[index] = Max(element, s.NodeId_version[index])
			} else {
				s.NodeId_version[index] = element
			}
		}
	}
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	for index,element := range s.NodeId_version {
		if _, ok := otherClock.NodeId_version[index];ok{
			if element != otherClock.NodeId_version[index] {
				return false
			}
		} else {
			if s.NodeId_version[index] != 0 {
				return false
			}
		}
	}
	return true
}

func Max(x, y uint64) uint64{
	if x < y {
		return y
	}
	return x
}
