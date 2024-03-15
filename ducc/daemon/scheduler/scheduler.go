package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type scheduledActionPQ[O any] struct {
	ops               []*ScheduledOperation[O]
	objectUseCount    map[ObjectIDType]int
	maxObjectUseCount int
}

type ObjectIDType = string
type ActionIDType = string
type OperationIDType = int

type ScheduledOperation[O any] struct {
	OperationID OperationIDType
	ObjectID    ObjectIDType
	ActionID    ActionIDType
	NextRun     time.Time
	index       int
	Obj         O
}

type Scheduler[O any] struct {
	pq         scheduledActionPQ[O]
	actionsMap map[ObjectIDType]map[ActionIDType]map[OperationIDType]*ScheduledOperation[O]
	lock       sync.Mutex
	idCounter  OperationIDType
	update     chan any
}

func NewScheduler[O any](maxObjectUseCount int) *Scheduler[O] {
	objectUseCount := make(map[ObjectIDType]int)
	s := &Scheduler[O]{
		pq:         scheduledActionPQ[O]{ops: make([]*ScheduledOperation[O], 0), objectUseCount: objectUseCount, maxObjectUseCount: maxObjectUseCount},
		actionsMap: make(map[ObjectIDType]map[ActionIDType]map[OperationIDType]*ScheduledOperation[O]),
		update:     make(chan any, 1),
	}

	return s
}

func (s *Scheduler[O]) notify() {
	select {
	case s.update <- nil:
	default:
	}

}

func (s *Scheduler[O]) IncrementObjectUseCount(objectId ObjectIDType) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.pq.objectUseCount[objectId]; !exists {
		s.pq.objectUseCount[objectId] = 0
	}

	s.pq.objectUseCount[objectId] = s.pq.objectUseCount[objectId] + 1
	// Need to fix the priority queue if the object just became busy and has operations scheduled
	if s.pq.objectUseCount[objectId] == s.pq.maxObjectUseCount && len(s.pq.ops) > 0 && (*s.pq.ops[0]).ObjectID == objectId {
		affectedActions := make([]ActionIDType, 0, 1)
		for actionType := range s.actionsMap[objectId] {
			affectedActions = append(affectedActions, actionType)
		}
		// Need to fix them sequentially, as the indexes can change during the fix
		for _, actionType := range affectedActions {
			for _, operation := range s.actionsMap[objectId][actionType] {
				heap.Fix(&s.pq, operation.index)
			}
		}
		s.notify()
	}
}

func (s *Scheduler[O]) DecrementObjectUseCount(objectId ObjectIDType) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.pq.objectUseCount[objectId]; !exists {
		return
	}
	s.pq.objectUseCount[objectId] = s.pq.objectUseCount[objectId] - 1
	// Need to fix the priority queue if the object just became free and has operations scheduled
	if s.pq.objectUseCount[objectId] == s.pq.maxObjectUseCount-1 && len(s.pq.ops) > 0 && s.pq.ops[0].ObjectID == objectId {
		affectedActions := make([]ActionIDType, 0, 1)
		for actionType := range s.actionsMap[objectId] {
			affectedActions = append(affectedActions, actionType)
		}
		// Need to fix them sequentially, as the indexes can change during the fix
		for _, actionType := range affectedActions {
			for _, operation := range s.actionsMap[objectId][actionType] {
				heap.Fix(&s.pq, operation.index)
			}
		}
		s.notify()
	}
}

func (s *Scheduler[O]) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.pq.ops)
}

func (s *Scheduler[O]) ObjectIsLocked(objectId ObjectIDType) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.pq.objectUseCount[objectId]; !exists {
		return false
	}
	return s.pq.objectUseCount[objectId] >= s.pq.maxObjectUseCount
}

func (s *Scheduler[O]) Schedule(obj O, objectId ObjectIDType, actionId ActionIDType, nextRun time.Time) {
	s.lock.Lock()
	defer s.lock.Unlock()

	operation := &ScheduledOperation[O]{
		OperationID: s.idCounter,
		ObjectID:    objectId,
		ActionID:    actionId,
		NextRun:     nextRun,
		Obj:         obj,
	}
	s.idCounter++

	_, exists := s.actionsMap[operation.ObjectID]
	if !exists {
		// No operations are scheduled for this entity yet
		s.actionsMap[operation.ObjectID] = make(map[ActionIDType]map[OperationIDType]*ScheduledOperation[O])
	}
	_, exists = s.actionsMap[operation.ObjectID][operation.ActionID]
	if !exists {
		// No operations of this type are scheduled for this entity yet
		s.actionsMap[operation.ObjectID][operation.ActionID] = make(map[OperationIDType]*ScheduledOperation[O])
	}

	// Add the new operation to the map and the priority queue
	s.actionsMap[operation.ObjectID][operation.ActionID][operation.OperationID] = operation
	heap.Push(&s.pq, operation)
	s.notify()
}

func (s *Scheduler[O]) Pop() *O {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.pq.ops) == 0 {
		return nil
	}

	s.notify()

	out := s.pq.ops[0].Obj
	heap.Pop(&s.pq)
	return &out
}

func (s *Scheduler[O]) Peek() *O {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.pq.ops) == 0 {
		return nil
	}

	return &s.pq.ops[0].Obj
}

func (s *Scheduler[O]) RemoveOperation(objectID ObjectIDType, actionID ActionIDType, operationID OperationIDType) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.actionsMap[objectID]; !exists {
		return
	}
	if _, exists := s.actionsMap[objectID][actionID]; !exists {
		return
	}
	delete(s.actionsMap[objectID][actionID], operationID)

	s.notify()
}

func (s *Scheduler[O]) RemoveActionForObject(objectId ObjectIDType, actionId ActionIDType) {
	s.lock.Lock()
	defer s.lock.Unlock()

	operations, exists := s.actionsMap[objectId][actionId]
	if !exists {
		return
	}

	for _, operation := range operations {
		heap.Remove(&s.pq, operation.index)
	}
	if len(s.actionsMap[objectId]) == 1 {
		// This was the last remaining action for this object, so we can remove the object from the map
		delete(s.actionsMap, objectId)
	} else {
		delete(s.actionsMap[objectId], actionId)
	}
	s.notify()
}

func (s *Scheduler[O]) RemoveObject(objectId ObjectIDType) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.actionsMap[objectId]; !exists {
		return
	}

	// Remove all operations for this entity from the map and the priority queue
	for actionID, operations := range s.actionsMap[objectId] {
		for _, operation := range operations {
			heap.Remove(&s.pq, operation.index)
		}
		delete(s.actionsMap[objectId], actionID)
	}
	delete(s.actionsMap, objectId)
	s.notify()
}

func (s *Scheduler[O]) GetOperationsForObjectAction(objectId ObjectIDType, actionId ActionIDType) []*ScheduledOperation[O] {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.actionsMap[objectId]; !exists {
		return nil
	}
	if _, exists := s.actionsMap[objectId][actionId]; !exists {
		return nil
	}

	operations := make([]*ScheduledOperation[O], 0, len(s.actionsMap[objectId][actionId]))
	for _, operation := range s.actionsMap[objectId][actionId] {
		operations = append(operations, operation)
	}
	return operations
}

func (s *Scheduler[O]) WaitForNextOperation(ctx context.Context) (obj *O, objectId ObjectIDType, actionId ActionIDType, nextRun time.Time) {
	var waitingForCheck *ScheduledOperation[O]
	var cancelTimer chan any
	s.notify()
	for {
		select {
		case <-ctx.Done():
			// We have been asked to stop waiting for the next check
			// We need to stop the timer goroutine if it is running
			if waitingForCheck != nil {
				close(cancelTimer)
			}
			return nil, "", "", time.Time{}
		case <-s.update:
			s.lock.Lock()
		}

		if waitingForCheck != nil {
			// We have been woken up while waiting for a check to be due.
			// This means that a timer goroutine has been running.

			if len(s.pq.ops) == 0 {
				// We have no more checks scheduled, so we can stop the timer goroutine
				close(cancelTimer)
				waitingForCheck = nil
				s.lock.Unlock()
				s.notify()
				continue
			}
			if s.pq.ops[0] != waitingForCheck {
				// The next check is not the one we were waiting for, so we stop the timer goroutine
				close(cancelTimer)
				waitingForCheck = nil
				s.lock.Unlock()
				s.notify()
				continue
			}
			// The next check is the one we were waiting for
			// We can stop the timer goroutine and continue processing it
			close(cancelTimer)
			waitingForCheck = nil
		}

		var nextCheck *ScheduledOperation[O] = nil
		if len(s.pq.ops) > 0 {
			nextCheck = s.pq.ops[0]
		}
		if nextCheck == nil {
			// We have no checks scheduled, so we wait for the priority queue to change before checking again
			s.lock.Unlock()
			continue
		}

		if nextCheck.NextRun.After(time.Now()) {
			// The next check is not due yet, so we wait for it to be due or for the priority queue to change
			// We use a goroutine to notify us when the check is due
			cancelTimer = make(chan any)
			go func(cancel <-chan any, wakeTime time.Time) {
				timer := time.NewTimer(time.Until(wakeTime))
				select {
				case <-cancel:
					// Proper cleanup of the timer
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
					s.notify()
				}
			}(cancelTimer, nextCheck.NextRun)
			// We set waitingForCheck so that we know that we are waiting for a check to be due
			waitingForCheck = nextCheck
			s.lock.Unlock()
			continue
		}

		if s.pq.objectUseCount[nextCheck.ObjectID] >= s.pq.maxObjectUseCount {
			// All objects must be busy, so we need to wait
			s.lock.Unlock()
			continue
		}

		// We have a check that is due, so we unschedule it and return it
		heap.Remove(&s.pq, nextCheck.index)
		delete(s.actionsMap[nextCheck.ObjectID], nextCheck.ActionID)
		s.lock.Unlock()

		return &nextCheck.Obj, nextCheck.ObjectID, nextCheck.ActionID, nextCheck.NextRun
	}
}

func (pq scheduledActionPQ[O]) Len() int { return len(pq.ops) }

func (pq scheduledActionPQ[O]) Less(i, j int) bool {
	iObjBusy := pq.objectUseCount[pq.ops[i].ObjectID] >= pq.maxObjectUseCount
	jObjBusy := pq.objectUseCount[pq.ops[j].ObjectID] >= pq.maxObjectUseCount
	// If one of the objects is busy, we want to schedule the other one first
	if iObjBusy != jObjBusy {
		return !iObjBusy
	}
	// return the earlier of the two
	return pq.ops[i].NextRun.Before(pq.ops[j].NextRun)
}

func (pq scheduledActionPQ[O]) Swap(i, j int) {
	pq.ops[i], pq.ops[j] = pq.ops[j], pq.ops[i]
	pq.ops[i].index = i
	pq.ops[j].index = j
}

func (q *scheduledActionPQ[O]) Push(a any) {
	action := a.(*ScheduledOperation[O])
	n := len(q.ops)
	action.index = n
	q.ops = append(q.ops, action)
}

func (q *scheduledActionPQ[O]) Pop() any {
	old := q.ops
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	q.ops = old[0 : n-1]
	return item
}
