package daemon

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

type ScheduledUpdate struct {
	Type     db.OperationType
	EntityId uuid.UUID
	NextRun  time.Time
	Details  string
	index    int
}

type ScheduledUpdates struct {
	priorityQueue scheduledChecksPQ
	checkMap      map[uuid.UUID]map[db.OperationType]*ScheduledUpdate
	cv            sync.Cond
}

var scheduledUpdates = NewScheduledUpdates()

func InitScheduler() error {

	// We perform all this in a transaction, so we can roll back if something goes wrong
	tx, err := db.GetTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// If any checks were running when the server was shut down, we need to restart them
	toRestart, err := db.GetChecksByStatus(tx, db.OP_STATUS_RUNNING)
	if err != nil {
		return err
	}
	for _, check := range toRestart {
		err := restartCheck(tx, check)
		if err != nil {
			panic("Unable to restart check: " + err.Error())
		}
	}

	// Schedule checks for all images and whishes.
	images, err := db.GetAllImages(tx)
	if err != nil {
		return err
	}
	wishes, err := db.GetAllWishes(tx)
	if err != nil {
		return err
	}

	for _, image := range images {
		if err := scheduleManifestCheck(tx, image); err != nil {
			return err
		}
	}
	for _, wish := range wishes {
		if err := scheduleExpandWildcardCheck(tx, wish); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func NewScheduledUpdates() *ScheduledUpdates {
	return &ScheduledUpdates{
		priorityQueue: make(scheduledChecksPQ, 0),
		checkMap:      make(map[uuid.UUID]map[db.OperationType]*ScheduledUpdate),
		cv:            sync.Cond{L: &sync.Mutex{}},
	}
}

func scheduler(ctx context.Context) {
	newChecks := make(chan *ScheduledUpdate)
	go func() {
		for {
			check := scheduledUpdates.WaitForNextCheck(ctx)
			if check == nil {
				return
			}
			newChecks <- check
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case check := <-newChecks:
			TriggerCheck(check.EntityId, check.Type, db.TRIGGER_TYPE_SCHEDULED, check.Details)
		}
	}
}

func (s *ScheduledUpdates) Schedule(check *ScheduledUpdate) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	s.priorityQueue.Push(check)

	entityChecks, exists := s.checkMap[check.EntityId]
	if !exists {
		// No checks for this entity have been scheduled yet. We need to create a map for it.
		entityChecks = make(map[db.OperationType]*ScheduledUpdate)
		s.checkMap[check.EntityId] = entityChecks
	} else {
		oldCheck, typeExists := entityChecks[check.Type]
		if typeExists {
			// A check of this type is already scheduled for this entity, so we need to unschedule it
			heap.Remove(&s.priorityQueue, oldCheck.index)
		}
	}

	// Add the new check to the map and the priority queue
	entityChecks[check.Type] = check
	heap.Push(&s.priorityQueue, check)

	s.cv.Broadcast()
}

func (s *ScheduledUpdates) Pop() *ScheduledUpdate {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	if len(s.priorityQueue) == 0 {
		return nil
	}

	s.cv.Broadcast()

	return heap.Pop(&s.priorityQueue).(*ScheduledUpdate)
}

func (s *ScheduledUpdates) Peek() *ScheduledUpdate {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	if len(s.priorityQueue) == 0 {
		return nil
	}

	return s.priorityQueue[0]
}

func (s *ScheduledUpdates) Remove(entityId uuid.UUID, checkType db.OperationType) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	item, exists := s.checkMap[entityId][checkType]
	if !exists {
		return
	}

	// Remove the check from the map and the priority queue
	heap.Remove(&s.priorityQueue, item.index)
	delete(s.checkMap[entityId], checkType)

	s.cv.Broadcast()
}

func (s *ScheduledUpdates) RemoveEntity(entityId uuid.UUID) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()

	entityChecks, exists := s.checkMap[entityId]
	if !exists {
		return
	}

	for _, check := range entityChecks {
		heap.Remove(&s.priorityQueue, check.index)
	}

	delete(s.checkMap, entityId)

	s.cv.Broadcast()
}

func (s *ScheduledUpdates) WaitForNextCheck(ctx context.Context) *ScheduledUpdate {
	var waitingForCheck *ScheduledUpdate
	var cancelTimer chan any
	s.cv.L.Lock()
	for {
		select {
		case <-ctx.Done():
			// We have been asked to stop waiting for the next check
			// We need to stop the timer goroutine if it is running
			if cancelTimer != nil {
				close(cancelTimer)
			}
			s.cv.L.Unlock()
			return nil
		default:
		}

		if waitingForCheck != nil {
			// We have been woken up while waiting for a check to be due.
			// This means that a timer goroutine has been running.
			// Either we have been woke up because the timer has expired, or because the priority queue has changed.
			// Need to handle both cases.

			if len(s.priorityQueue) == 0 {
				// We have no more checks scheduled, so we can stop the timer goroutine
				// The next iteration of the loop will wait for the priority queue to change
				close(cancelTimer)
				waitingForCheck = nil
				continue
			}
			if s.priorityQueue[0] != waitingForCheck {
				// The next check is not the one we were waiting for, so we stop the timer goroutine
				// The next check will be processed by the next iteration of the loop
				close(cancelTimer)
				waitingForCheck = nil
				continue
			}
			// The next check is the one we were waiting for
			// We can stop the timer goroutine and continue processing it
			close(cancelTimer)
		}

		var nextCheck *ScheduledUpdate = nil
		if len(s.priorityQueue) > 0 {
			nextCheck = s.priorityQueue[0]
		}
		if nextCheck == nil {
			// We have no checks scheduled, so we wait for the priority queue to change before checking again
			s.cv.Wait()
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
					s.cv.Broadcast()
				}
			}(cancelTimer, nextCheck.NextRun)
			// We set waitingForCheck so that we know that we are waiting for a check to be due
			waitingForCheck = nextCheck
			s.cv.Wait()
			continue
		}

		// We have a check that is due, so we unschedule it and return it
		heap.Remove(&s.priorityQueue, nextCheck.index)
		delete(s.checkMap[nextCheck.EntityId], nextCheck.Type)
		s.cv.L.Unlock()
		return nextCheck
	}
}

type scheduledChecksPQ []*ScheduledUpdate

func (q scheduledChecksPQ) Len() int { return len(q) }

func (q scheduledChecksPQ) Less(i, j int) bool {
	return q[i].NextRun.Before(q[j].NextRun)
}

func (q scheduledChecksPQ) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *scheduledChecksPQ) Push(check any) {
	item, ok := check.(*ScheduledUpdate)
	if !ok {
		panic("tried to push something that is not a *ScheduledCheck")
	}
	n := len(*q)
	item.index = n
	*q = append(*q, item)
}

func (q *scheduledChecksPQ) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*q = old[0 : n-1]
	return item
}

func scheduleExpandWildcardCheck(tx *sql.Tx, wish db.Wish) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// Find out when the last completed check for this wish was
	lastCheck, err := db.GetLastCheckForEntityIDFilterByTypeAndStatus(nil, wish.ID, []db.OperationType{db.OP_TYPE_EXPAND_WILDCARDS}, []db.CheckStatus{db.OP_STATUS_SUCCESS, db.CHECK_STATUS_FAILED})
	if err != nil {
		return nil
	}

	// Calculate the next check time
	nextCheckTime := lastCheck.DoneTimestamp.Add(time.Duration(wish.ScheduleOptions.ExpandWildcardInterval.Value) * time.Second)
	if nextCheckTime.Before(time.Now()) {
		nextCheckTime = time.Now()
	}

	// Create and schedule the check
	checkToSchedule := ScheduledUpdate{
		Type:     db.OP_TYPE_EXPAND_WILDCARDS,
		EntityId: wish.ID,
		NextRun:  nextCheckTime,
		Details:  "",
	}

	scheduledUpdates.Schedule(&checkToSchedule)

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil

}

func scheduleManifestCheck(tx *sql.Tx, image db.Image) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// Find all whishes that match this image
	wishes, err := db.GetWishesByImageID(tx, image.ID)
	if err != nil {
		return err
	}

	if len(wishes) == 0 {
		// No wishes reference this image, so we don't want to check it
		return nil
	}

	// Out of all the wishes, find the one with the lowest update interval
	var shortestIntervalWish db.Wish
	for i, wish := range wishes {
		if i == 0 {
			shortestIntervalWish = wish
		} else if wish.ScheduleOptions.CheckImageInfoInterval.Value < shortestIntervalWish.ScheduleOptions.CheckImageInfoInterval.Value {
			shortestIntervalWish = wish
		}
	}

	// Find out when the last completed check for this image was
	lastCheck, err := db.GetLastCheckForEntityIDFilterByTypeAndStatus(tx, image.ID, []db.OperationType{db.OP_TYPE_UPDATE_IMAGE}, []db.CheckStatus{db.OP_STATUS_SUCCESS, db.CHECK_STATUS_FAILED})
	if err != nil {
		return err
	}

	// Done with the DB, commit the transaction
	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	// Calculate the next check time
	nextCheckTime := lastCheck.DoneTimestamp.Add(time.Duration(shortestIntervalWish.ScheduleOptions.CheckImageInfoInterval.Value) * time.Second)
	if nextCheckTime.Before(time.Now()) {
		nextCheckTime = time.Now()
	}

	// Create and schedule the check
	checkToSchedule := ScheduledUpdate{
		Type:     db.OP_TYPE_UPDATE_IMAGE,
		EntityId: image.ID,
		NextRun:  nextCheckTime,
		Details:  fmt.Sprintf("Wanted by wish %s", shortestIntervalWish.ID.String()),
	}

	scheduledUpdates.Schedule(&checkToSchedule)

	return nil
}

func rescheduleCheck(entityID uuid.UUID, checkType db.OperationType) error {
	switch checkType {
	case db.OP_TYPE_EXPAND_WILDCARDS:
		// Get the associated wish
		wish, err := db.GetWishByID(nil, entityID)
		if err != nil {
			return err
		}
		if err := scheduleExpandWildcardCheck(nil, wish); err != nil {
			return err
		}
	case db.OP_TYPE_UPDATE_IMAGE:
		// Get the associated image
		image, err := db.GetImageByID(nil, entityID)
		if err != nil {
			return err
		}
		if err := scheduleManifestCheck(nil, image); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown check type %s", checkType)

	}
	return nil
}
