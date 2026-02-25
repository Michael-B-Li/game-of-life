package main

import (
	"net/rpc"
	"os"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

/* WORKER STATE AND HALO STRUCT */

type GameWorker struct {
	mu              sync.RWMutex
	slice           [][]byte
	width           int
	totalHeight     int
	startRow        int
	topNeighbour    string
	bottomNeighbour string
	topClient       *rpc.Client
	bottomClient    *rpc.Client
	threads         int

	haloMu        sync.Mutex
	haloExchanges map[int]*haloExchange
}

type haloExchange struct {
	top    chan []byte
	bottom chan []byte
}

/* PUBLIC RPC METHODS */

// Sets up worker with its portion of the world and neighbour's stuff
func (w *GameWorker) Initialise(req gol.WorkerInitRequest, res *gol.WorkerInitResponse) error {
	// Store worker's partition and data
	w.mu.Lock()
	w.slice = copyWorld(req.Slice)
	w.width = req.Width
	w.totalHeight = req.TotalHeight
	w.startRow = req.StartRow
	// Clean up old neighbour connections
	if w.topNeighbour != req.TopNeighbour && w.topClient != nil {
		w.topClient.Close()
		w.topClient = nil
	}
	if w.bottomNeighbour != req.BottomNeighbour && w.bottomClient != nil {
		w.bottomClient.Close()
		w.bottomClient = nil
	}
	// Store new neighbour addresses
	w.topNeighbour = req.TopNeighbour
	w.bottomNeighbour = req.BottomNeighbour

	if w.haloExchanges == nil {
		w.haloExchanges = make(map[int]*haloExchange)
	}

	w.threads = determineThreadCount(len(req.Slice))

	w.mu.Unlock()
	return nil
}

// The broker calls this every turn to make a worker compute its next generation
func (w *GameWorker) Evolve(req gol.WorkerEvolveRequest, res *gol.WorkerEvolveResponse) error {
	w.mu.RLock()
	// Copies the current slice into localSlice and capture all metadata
	localSlice := copyWorld(w.slice)
	totalHeight := w.totalHeight
	startRow := w.startRow
	topNeighbour := w.topNeighbour
	bottomNeighbour := w.bottomNeighbour
	w.mu.RUnlock() // Release lock here so worker can handle other RPCs while computing

	if len(localSlice) == 0 {
		return nil
	}
	sliceWidth := len(localSlice[0])

	// Exchange halo rows with neighbours here, sending worker's top row to top neighbour and likewise with bottom
	// Receives halos from both neighbours
	topHalo, bottomHalo, err := w.exchangeHalos(req.Turn, localSlice, topNeighbour, bottomNeighbour)
	if err != nil {
		return err
	}

	// Build buffered world with halos, buffered[0] is topHalo and buffered[len-1] is bottomHalo
	buffered := make([][]byte, len(localSlice)+2)
	if len(localSlice) > 0 {
		buffered[0] = topHalo
		for i := range localSlice {
			buffered[i+1] = localSlice[i]
		}
		buffered[len(buffered)-1] = bottomHalo
	} else {
		buffered[0] = make([]byte, sliceWidth)
		buffered[1] = make([]byte, sliceWidth)
	}

	// Compute the next state using calculateNextState on the buffered slice, and write result into a non-buffered nextSlice
	nextSlice := makeWorld(len(localSlice), sliceWidth)
	threads := w.threads
	if threads < 1 {
		threads = determineThreadCount(len(localSlice))
		w.threads = threads
	}
	delta, flippedCells := calculateNextState(buffered, nextSlice, sliceWidth, req.CollectFlips, startRow, totalHeight, threads)

	w.mu.Lock()
	w.slice = nextSlice
	w.mu.Unlock()
	// Fill the change in alive cells in RPC response
	res.Delta = delta
	if req.CollectFlips {
		res.FlippedCells = flippedCells
	}

	return nil
}

// Receives a halo row from a neighbour worker
func (w *GameWorker) ReceiveHalo(req gol.WorkerHaloDelivery, res *gol.WorkerHaloAck) error {
	// Copy the incoming row because callers reuse the backing array
	rowCopy := append([]byte(nil), req.Row...)
	// find the halo exchange channels for this turn
	exchange := w.haloExchangeForTurn(req.Turn)

	switch req.Direction {
	case "top":
		exchange.top <- rowCopy
	case "bottom":
		exchange.bottom <- rowCopy
	}

	return nil
}

// Acquires a read lock, and makes a copy (prevent data race) of the worker's current slice
func (w *GameWorker) GetSlice(req gol.WorkerSliceRequest, res *gol.WorkerSliceResponse) error {
	w.mu.RLock()
	res.Slice = copyWorld(w.slice)
	w.mu.RUnlock()
	return nil
}

// Shutdown terminates the worker process after acknowledging the request
func (w *GameWorker) Shutdown(req gol.WorkerStopRequest, res *gol.WorkerStopResponse) error {
	w.mu.Lock()
	if w.topClient != nil {
		w.topClient.Close()
		w.topClient = nil
	}
	if w.bottomClient != nil {
		w.bottomClient.Close()
		w.bottomClient = nil
	}
	w.mu.Unlock()

	go func() {
		time.Sleep(100 * time.Millisecond)
		os.Exit(0)
	}()
	return nil
}

/* HALO RPC SUPPORTING FUNCTIONS*/

func (w *GameWorker) exchangeHalos(turn int, localSlice [][]byte, topNeighbour, bottomNeighbour string) ([]byte, []byte, error) {
	// Creates the halo exchange struct for this turn
	exchange := w.haloExchangeForTurn(turn)

	// Helper function to send a row copy to a channel
	deliver := func(ch chan []byte, row []byte) {
		ch <- append([]byte(nil), row...)
	}

	errCh := make(chan error, 2)
	pending := 0
	// Copy first row and launch goroutine to send it via RPC to top neighbour
	if topNeighbour != "" {
		pending++
		row := append([]byte(nil), localSlice[0]...)
		go func() {
			errCh <- w.sendHaloRow("top", "bottom", turn, row)
		}()
	} else {
		deliver(exchange.top, localSlice[len(localSlice)-1])
	}
	// Copy last row and launch goroutine to send it via RPC to bottom neighbour
	if bottomNeighbour != "" {
		pending++
		row := append([]byte(nil), localSlice[len(localSlice)-1]...)
		go func() {
			errCh <- w.sendHaloRow("bottom", "top", turn, row)
		}()
	} else {
		deliver(exchange.bottom, localSlice[0])
	}

	for pending > 0 {
		if transmitErr := <-errCh; transmitErr != nil {
			w.releaseHaloExchange(turn)
			return nil, nil, transmitErr
		}
		pending--
	}
	// Receives halos from neighbours
	topHalo := <-exchange.top
	bottomHalo := <-exchange.bottom
	// Cleans up exchange and returns the halo rows
	w.releaseHaloExchange(turn)
	return topHalo, bottomHalo, nil
}

// send boundary row to neighbour worker via RPC (halo), used to calculate edge cells by that neighbour for next gol turn
func (w *GameWorker) sendHaloRow(neighbour string, direction string, turn int, row []byte) error {
	client, err := w.ensureNeighbourClient(neighbour)
	if err != nil {
		return err
	}
	if client == nil {
		return nil
	}

	req := gol.WorkerHaloDelivery{
		Turn:      turn,
		Direction: direction,
		Row:       append([]byte(nil), row...),
	}

	var ack gol.WorkerHaloAck
	return client.Call("GameWorker.ReceiveHalo", req, &ack)
}

// Gets or creates synchronisation channels for a specific turn
func (w *GameWorker) haloExchangeForTurn(turn int) *haloExchange {
	w.haloMu.Lock()
	if w.haloExchanges == nil {
		w.haloExchanges = make(map[int]*haloExchange)
	}

	exchange, ok := w.haloExchanges[turn]
	if !ok {
		exchange = &haloExchange{
			top:    make(chan []byte, 1),
			bottom: make(chan []byte, 1),
		}
		w.haloExchanges[turn] = exchange
	}

	w.haloMu.Unlock()
	return exchange
}

// To clean up the halo exchange state when a turn completed
func (w *GameWorker) releaseHaloExchange(turn int) {
	w.haloMu.Lock()
	delete(w.haloExchanges, turn)
	w.haloMu.Unlock()
}

/* COMPUTATION */

func calculateNextState(world [][]byte, nextWorld [][]byte, width int, collectFlips bool, startRow int, totalHeight int, threads int) (int, []util.Cell) {
	rows := len(nextWorld)
	if rows == 0 {
		return 0, nil
	}
	// No parallelisation due to 1 thread
	if threads < 2 {
		return calculateSubSlice(world, nextWorld, width, collectFlips, startRow, totalHeight, 0, rows)
	}
	if threads > rows {
		threads = rows
	}

	type sliceResult struct { // each worker returns a sliceResult
		delta int
		flips []util.Cell
	}

	rPT := (rows + threads - 1) / threads
	results := make(chan sliceResult, threads) // create channel to send sliceResults

	var wg sync.WaitGroup // wait until all finished

	for startIdx := 0; startIdx < rows; startIdx += rPT { // divide the workers slice for each thread
		endIdx := startIdx + rPT
		if endIdx > rows {
			endIdx = rows
		}

		from, to := startIdx, endIdx
		wg.Add(1)
		go func() {
			delta, flips := calculateSubSlice(world, nextWorld, width, collectFlips, startRow, totalHeight, from, to)
			results <- sliceResult{delta: delta, flips: flips}
			wg.Done()
		}()
	}

	wg.Wait() // can finally continue after workers done and then close channel
	close(results)

	// total results from all the threads
	totalDelta := 0
	var merged []util.Cell
	if collectFlips {
		merged = make([]util.Cell, 0, rows*width/4)
	}

	for res := range results {
		totalDelta += res.delta
		if collectFlips && len(res.flips) > 0 {
			merged = append(merged, res.flips...)
		}
	}

	return totalDelta, merged
}

func calculateSubSlice(world [][]byte, nextWorld [][]byte, width int, collectFlips bool, startRow int, totalHeight int, rowStart, rowEnd int) (int, []util.Cell) {
	birthsMinusDeaths := 0
	var cells []util.Cell
	if collectFlips {
		subSize := (rowEnd - rowStart) * width
		cells = make([]util.Cell, 0, subSize/4)
	}

	for localRow := rowStart; localRow < rowEnd; localRow++ {
		bufferIndex := localRow + 1 // add 1 to the pointer to account for halo offset
		// gets the 3 rows to calculate gol neighbours
		topRow := world[bufferIndex-1]
		curRow := world[bufferIndex]
		bottomRow := world[bufferIndex+1]
		// handles wraparound
		globalRow := localRow
		if totalHeight > 0 {
			globalRow = startRow + localRow // row index in full world = row index in this worker + global row index of worker's first real row
			if globalRow >= totalHeight {
				globalRow -= totalHeight
			}
		}

		for u := 0; u < width; u++ {
			// Calculate horizontal neighbour indices
			left := u - 1
			if left < 0 {
				left += width
			}
			right := u + 1
			if right == width {
				right = 0
			}
			// sum alive
			sum := int(topRow[left]) + int(topRow[u]) + int(topRow[right]) +
				int(curRow[left]) + int(curRow[right]) +
				int(bottomRow[left]) + int(bottomRow[u]) + int(bottomRow[right])

			if curRow[u] == 255 {
				if sum < 510 || sum > 765 { // dead
					nextWorld[localRow][u] = 0
					birthsMinusDeaths--
					if collectFlips {
						cells = append(cells, util.Cell{X: u, Y: globalRow})
					}
				} else { // stay alive
					nextWorld[localRow][u] = 255
				}
			} else {
				if sum == 765 { // become alive
					nextWorld[localRow][u] = 255
					birthsMinusDeaths++
					if collectFlips {
						cells = append(cells, util.Cell{X: u, Y: globalRow})
					}
				} else {
					nextWorld[localRow][u] = 0 // stay dead
				}
			}
		}
	}

	return birthsMinusDeaths, cells
}

/* NETWORKING HELPERS */

func (w *GameWorker) ensureNeighbourClient(neighbour string) (*rpc.Client, error) {
	// re-use of a cached rpc.Client if already dialed for worker so we don't spam connections
	addr, existing := w.getNeighbour(neighbour)
	// no neighbour row (topmost or bottommost worker)
	if addr == "" {
		return nil, nil
	}
	// re-use
	if existing != nil {
		return existing, nil
	}
	// dial new connection
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	// store for future re-use
	w.storeNeighbourClient(neighbour, client)
	return client, nil
}

// retrieves address and RPC client for neighbour worker, neighbour either top or bottom
func (w *GameWorker) getNeighbour(neighbour string) (string, *rpc.Client) {
	w.mu.RLock()
	if neighbour == "top" {
		addr := w.topNeighbour
		client := w.topClient
		w.mu.RUnlock()
		return addr, client
	}
	addr := w.bottomNeighbour
	client := w.bottomClient
	w.mu.RUnlock()
	return addr, client
}

// cache an RPC client connection to a neighbour worker, avoiding a re-dial, neighbour either top or bottom
func (w *GameWorker) storeNeighbourClient(neighbour string, client *rpc.Client) {
	w.mu.Lock()
	if neighbour == "top" {
		w.topClient = client
		w.mu.Unlock()
		return
	}
	w.bottomClient = client
	w.mu.Unlock()
}

/* WORLD HELPERS */

// Create copy of the world
func copyWorld(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = append([]byte(nil), in[i]...)
	}
	return out
}

// Create 2d slice filled with 0, used in Evolve (nextSlice)
func makeWorld(height, width int) [][]byte {
	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	return world
}

// determine n.o of threads to use to prevent wastage
func determineThreadCount(rows int) int {
	const maxThreads = 8
	if rows <= 0 {
		return 1
	}
	if rows < maxThreads {
		return rows
	}
	return maxThreads
}
