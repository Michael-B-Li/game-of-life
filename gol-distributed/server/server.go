package main

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

/* BROKER STATE AND WORLD PARTITION TYPE */
// GolWorker is the RPC server that processes Game of Life turns
type Broker struct { // Stateful, tracks current state of processing
	mu               sync.RWMutex // Mutex lock prevents race condition from ProcessTurns and GetAliveCellsCount
	cachedWorld      [][]byte
	currentTurn      int
	totalTurns       int
	processing       bool
	aliveCellsCount  int
	paused           bool
	shutdown         bool
	height           int
	width            int
	resumeCh         chan struct{} // Closed to resume when paused, recreated on pause, -> no busy waiting
	shutdownCh       chan struct{} // Closed to signal shutdown
	doneCh           chan struct{} // Closed when processing completes
	partitionStarts  []int         // Starting row index in global world for worker i's slice
	partitionLengths []int         // Number of rows assigned to worker i

	workerAddresses []string
}

// Tracks a worker's assigned slice of the world
type partitionInfo struct {
	start int      // starting row in the global world
	slice [][]byte // the actual rows assigned to this worker
}

/* MAIN RPC ENDPOINT */
// ProcessTurns evolves the Game of Life for the specified number of turns, returning a final world state
func (b *Broker) ProcessTurns(req gol.Request, res *gol.Response) error {

	// sets up world height and width
	height := req.Height
	width := req.Width

	// sets up worker addresses
	workerAddresses := b.workerAddresses
	workerCount := len(workerAddresses)

	// sets up rpc calls
	workerClients := make([]*rpc.Client, workerCount)
	for i, addr := range workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("error dialing worker %s: %w", addr, err)
		}
		workerClients[i] = client
		defer client.Close()
	}

	// splits world
	partitions := partitionWorld(req.World, workerCount)
	for i, part := range partitions {
		if len(part.slice) == 0 {
			return fmt.Errorf("worker %d assigned empty slice; reduce number of workers or increase world height", i)
		}
	}
	starts := make([]int, workerCount)
	lengths := make([]int, workerCount)

	for i, client := range workerClients {

		// determine top and bottom neighbour addresses for this worker
		topNeighbour := ""
		bottomNeighbour := ""
		if workerCount > 1 {
			topNeighbour = workerAddresses[(i-1+workerCount)%workerCount]
			bottomNeighbour = workerAddresses[(i+1)%workerCount]
		}

		part := partitions[i]
		starts[i] = part.start
		lengths[i] = len(part.slice)

		// sets up initial request
		initReq := gol.WorkerInitRequest{
			Slice:           part.slice,
			Width:           width,
			TotalHeight:     height,
			StartRow:        part.start,
			WorkerIndex:     i,
			TotalWorkers:    workerCount,
			TopNeighbour:    topNeighbour,
			BottomNeighbour: bottomNeighbour,
		}

		// calls the initial configuration
		var initRes gol.WorkerInitResponse
		if err := client.Call("GameWorker.Initialise", initReq, &initRes); err != nil {
			return fmt.Errorf("initialising worker %d failed: %w", i, err)
		}
	}

	// sets up broker variables
	initialWorld := copyWorld(req.World)
	b.mu.Lock()
	b.cachedWorld = initialWorld
	b.currentTurn = 0
	b.totalTurns = req.Turns
	b.processing = true
	b.paused = false
	b.shutdown = false
	b.height = height
	b.width = width
	b.aliveCellsCount = countAliveCells(initialWorld)
	b.resumeCh = make(chan struct{})
	b.shutdownCh = make(chan struct{})
	b.doneCh = make(chan struct{})
	b.partitionStarts = starts
	b.partitionLengths = lengths
	b.mu.Unlock()

	// When the controller exposes a live-view endpoint, keep a connection open so we can stream flips.
	var viewerClient *rpc.Client
	if addr := req.LiveViewAddr; addr != "" {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("live view disabled: %v\n", err)
		} else {
			viewerClient = client
			defer viewerClient.Close()
		}
	}
	// Only ask workers for flipped cells if someone is listening
	collectFlips := viewerClient != nil

	fmt.Println("\nCurrent workers: ", workerAddresses)
	// iterates through the turns
	for turn := 0; turn < req.Turns; turn++ {
		workerCount = len(workerClients)
		var turnFlips []util.Cell

		// sets up pause, resume and shutdown variables
		b.mu.RLock()
		isPaused := b.paused
		resumeCh := b.resumeCh
		shutdownCh := b.shutdownCh
		isShutdown := b.shutdown
		b.mu.RUnlock()

		// checks if shutdown
		if isShutdown {
			break
		}

		// checks if paused
		if isPaused {
			select {
			case <-resumeCh: // wait here until Resume() closes resumeCh, then continue
			case <-shutdownCh:
				b.mu.Lock()
				b.shutdown = true
				b.mu.Unlock()
				continue
			}
			// Double-check shutdown after unblock, so we consider
			// a shutdown that happens right after resume, preventing another turn after shutdown req
			b.mu.RLock()
			if b.shutdown {
				b.mu.RUnlock()
				break
			}
			b.mu.RUnlock()
		}

		// creates replies and calls array
		replies := make([]gol.WorkerEvolveResponse, workerCount)
		calls := make([]*rpc.Call, workerCount)

		// rpc calls Evolve for each of the clients
		for i, client := range workerClients {
			stepReq := gol.WorkerEvolveRequest{ // sets up each request
				Turn:         turn,
				CollectFlips: collectFlips,
			}
			calls[i] = client.Go("GameWorker.Evolve", stepReq, &replies[i], nil)
		}

		totalDelta := 0

		// iterates through the calls
		for i, call := range calls {
			// waits till call is done
			<-call.Done

			if call.Error != nil { // checks if call returns an error
				return fmt.Errorf("error from worker %d: %w", i, call.Error)
			}

			totalDelta += replies[i].Delta
			// If the worker changed something, record the flipped cells for live view
			// turnFlips is the combined list for this turn from all workers
			if collectFlips && len(replies[i].FlippedCells) > 0 {
				turnFlips = append(turnFlips, replies[i].FlippedCells...)
			}

		}

		b.mu.Lock()
		b.currentTurn = turn + 1
		b.aliveCellsCount += totalDelta
		b.mu.Unlock()

		if viewerClient != nil {
			update := gol.LiveViewerUpdate{
				Turn:         turn + 1,
				FlippedCells: turnFlips,
			}
			var ack gol.LiveViewerAck
			if err := viewerClient.Call("LiveView.Update", update, &ack); err != nil {
				fmt.Printf("live view updates failed: %v\n", err)
				viewerClient.Close()
				viewerClient = nil
				collectFlips = false
			}
		}

	}

	// recreates the world from the slices sent by the workers
	finalWorld, err := assembleWorldFromClients(workerClients, partitions, width, height)
	if err != nil {
		return err
	}

	// calculates the alive cells
	aliveCells := calculateAliveCells(finalWorld)

	b.mu.Lock()
	b.cachedWorld = finalWorld
	b.processing = false
	if b.doneCh != nil {
		close(b.doneCh)
	}
	b.mu.Unlock()

	// Fill RPC response
	res.World = finalWorld
	res.AliveCells = aliveCells
	return nil
}

/* I/O HELPERS */
// GetAliveCellsCount responds with the current number of alive cells and completed turns
func (b *Broker) GetAliveCellsCount(req gol.AliveCellsRequest, res *gol.AliveCellsResponse) error {
	b.mu.RLock()
	res.CellsCount = b.aliveCellsCount
	res.CompletedTurns = b.currentTurn
	b.mu.RUnlock()
	return nil
}

// Pause responds to the request by pausing processing, via updating the paused flag and creation of resume channel
func (b *Broker) Pause(req gol.PauseRequest, res *gol.PauseResponse) error {
	b.mu.Lock()
	if !b.paused {
		b.paused = true
		// Create a new resume channel to block waiters
		b.resumeCh = make(chan struct{})
	}
	res.Turn = b.currentTurn
	b.mu.Unlock()
	return nil
}

// Resume responds to the request by resuming processing, closing the resume channel to wake waiters, unblocking access
func (b *Broker) Resume(req gol.ResumeRequest, res *gol.ResumeResponse) error {
	b.mu.Lock()
	if b.paused {
		close(b.resumeCh)
		b.paused = false
	}
	b.mu.Unlock()
	return nil
}

// GetCurrentState responds with a snapshot of the broker's current state - a copy of the world, alive cells and current turn
// StateRequest is when user asks to save or quit
func (b *Broker) GetCurrentState(req gol.StateRequest, res *gol.StateResponse) error {
	// Added retry loop so Broker.GetCurrentState only returns a snapshot when b.currentTurn is the same
	// before and after rebuilding the world. this ensures reported turn always matches the image we write out
	for {
		b.mu.RLock()
		// Copy the current world, prevent data races
		worldCopy := copyWorld(b.cachedWorld)
		turn := b.currentTurn
		processing := b.processing
		addresses := append([]string(nil), b.workerAddresses...)
		starts := append([]int(nil), b.partitionStarts...)
		lengths := append([]int(nil), b.partitionLengths...)
		height := b.height
		width := b.width
		b.mu.RUnlock()
		// Using the copied information, ask each worker for it's current slice via RPC, and assembles them
		if processing {
			collected, err := fetchWorldFromWorkers(addresses, starts, lengths, width, height)
			if err != nil {
				return err
			}
			worldCopy = collected

			b.mu.RLock()
			if turn != b.currentTurn {
				b.mu.RUnlock()
				// The turn advanced while we were rebuilding the world, retry to keep
				// the snapshot consistent with the reported turn.
				continue
			}
			b.mu.RUnlock()
		}
		// Fill reponse with info from either the broker's cached copy or the new assembled world
		res.World = worldCopy
		res.AliveCells = calculateAliveCells(worldCopy)
		res.Turn = turn
		return nil
	}
}

// Shutdown signals the worker to shutdown and returns the final state
func (b *Broker) Shutdown(req gol.ShutdownRequest, res *gol.ShutdownResponse) error {
	b.mu.Lock()
	if !b.shutdown {
		b.shutdown = true
		if b.shutdownCh != nil {
			close(b.shutdownCh)
		}
	}
	doneCh := b.doneCh
	b.mu.Unlock()

	// Block until processing completes without busy wait
	if doneCh != nil {
		<-doneCh
	}

	b.mu.RLock()
	worldCopy := copyWorld(b.cachedWorld)
	turn := b.currentTurn
	addresses := append([]string(nil), b.workerAddresses...)
	starts := append([]int(nil), b.partitionStarts...)
	lengths := append([]int(nil), b.partitionLengths...)
	height := b.height
	width := b.width
	b.mu.RUnlock()

	if worldCopy == nil {
		collected, err := fetchWorldFromWorkers(addresses, starts, lengths, width, height)
		if err != nil {
			return err
		}
		worldCopy = collected
	}

	res.World = worldCopy
	res.AliveCells = calculateAliveCells(worldCopy)
	res.Turn = turn
	return nil
}

// Kill stops all workers and exits the broker process
func (b *Broker) Kill(req gol.KillRequest, res *gol.KillResponse) error {
	stopReq := gol.WorkerStopRequest{}
	for _, addr := range b.workerAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("error dialing worker %s for termination: %v\n", addr, err)
			continue
		}
		var stopRes gol.WorkerStopResponse
		if err := client.Call("GameWorker.Shutdown", stopReq, &stopRes); err != nil {
			fmt.Printf("error terminating worker %s: %v\n", addr, err)
		}
		client.Close()
	}

	time.AfterFunc(100*time.Millisecond, func() {
		os.Exit(0)
	})

	return nil
}

/* WORLD HELPERS */
// countAliveCells returns the count of alive cells
func countAliveCells(world [][]byte) int {
	count := 0
	for _, row := range world {
		for _, cell := range row {
			if cell == 255 {
				count++
			}
		}
	}
	return count
}

// calculateAliveCells returns a slice of all alive cells
func calculateAliveCells(world [][]byte) []util.Cell {
	cells := []util.Cell{}

	for i := 0; i < len(world); i++ {
		for u := 0; u < len(world[i]); u++ {
			if world[i][u] == 255 {
				cells = append(cells, util.Cell{X: u, Y: i})
			}
		}
	}
	return cells
}

// partitionWorld divides the world into roughly equal slices for each worker
// When rows don't divide evenly, earlier workers get one extra row
func partitionWorld(world [][]byte, workers int) []partitionInfo {
	height := len(world)
	baseRows := height / workers
	remainder := height % workers

	partitions := make([]partitionInfo, workers)
	start := 0
	for i := 0; i < workers; i++ {
		rows := baseRows
		// First 'remainder' workers get one extra row for load balancing
		if i < remainder {
			rows++
		}
		end := start + rows
		partitions[i] = partitionInfo{
			start: start,
			slice: copyWorldRange(world, start, end),
		}
		start = end
	}
	return partitions
}

// assembleWorldFromClients reconstructs the full world by fetching each worker's
// current slice via RPC and stitching them together in order.
// Called at the end of ProcessTurns to get the final result.
func assembleWorldFromClients(clients []*rpc.Client, partitions []partitionInfo, width, height int) ([][]byte, error) {
	world := worldCreation(height, width)
	for i, client := range clients {
		var resp gol.WorkerSliceResponse
		if err := client.Call("GameWorker.GetSlice", gol.WorkerSliceRequest{}, &resp); err != nil {
			return nil, fmt.Errorf("fetching slice from worker %d failed: %w", i, err)
		}
		part := partitions[i]
		// Copy each row from the worker's slice into the correct position in the full world
		for row := range resp.Slice {
			copy(world[part.start+row], resp.Slice[row])
		}
	}
	return world, nil
}

// copyWorld creates a copy of the entire world
func copyWorld(world [][]byte) [][]byte {
	if world == nil {
		return nil
	}
	return copyWorldRange(world, 0, len(world))
}

// Used to extract a worker's partition from the full world during initialisation
func copyWorldRange(world [][]byte, start, end int) [][]byte {
	if start >= end {
		return [][]byte{}
	}
	slice := make([][]byte, end-start)
	for i := start; i < end; i++ {
		slice[i-start] = append([]byte(nil), world[i]...)
	}
	return slice
}

// fetchWorldFromWorkers rebuilds the full world by querying workers via RPC
// Used for mid-run snapshots (save/quit) when processing is still active
func fetchWorldFromWorkers(addresses []string, starts, lengths []int, width, height int) ([][]byte, error) {
	world := worldCreation(height, width)
	for i, addr := range addresses {
		// Dial each worker fresh (not using persistent broker connections)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("dial worker %s: %w", addr, err)
		}
		var resp gol.WorkerSliceResponse
		callErr := client.Call("GameWorker.GetSlice", gol.WorkerSliceRequest{}, &resp)
		client.Close()
		if callErr != nil {
			return nil, fmt.Errorf("get slice from worker %s: %w", addr, callErr)
		}
		// Determine where this worker's slice belongs in the global world
		start := starts[i]
		length := len(resp.Slice)
		if lengths[i] < length {
			length = lengths[i]
		}
		// Copy rows into the correct position
		for row := 0; row < length; row++ {
			copy(world[start+row], resp.Slice[row])
		}
	}
	return world, nil
}

// creates a new world using specified height and width
func worldCreation(height int, width int) [][]byte {
	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	return world
}
