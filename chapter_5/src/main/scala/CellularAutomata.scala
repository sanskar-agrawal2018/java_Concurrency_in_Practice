/*
 * Cellular Automata using CyclicBarrier
 *
 * This example demonstrates a classic parallel cellular automata simulation.
 * Multiple worker threads each process a subboard of the grid, then synchronize
 * at a barrier before the next generation. The barrier action (run on the last
 * arriver's thread) commits all new values atomically.
 *
 * Key patterns:
 *  - Divide data into non-overlapping subboards (one per available CPU)
 *  - Each worker computes new values for its subboard
 *  - CyclicBarrier ensures all workers finish before committing
 *  - The barrier action runs atomically (no other workers are running)
 *  - Repeat until convergence
 */

import java.util.concurrent.{BrokenBarrierException, CyclicBarrier}
import scala.annotation.tailrec
import scala.util.Using

/**
 * Trait representing a Board in cellular automata.
 */
trait Board {
  def getMaxX: Int
  def getMaxY: Int
  def getValue(x: Int, y: Int): Int
  def setNewValue(x: Int, y: Int, value: Int): Unit
  def commitNewValues(): Unit
  def hasConverged: Boolean
  def getSubBoard(numPartitions: Int, partitionIdx: Int): Board
  def waitForConvergence(): Unit
}

/**
 * A simple Conway's Game of Life-like implementation.
 * Cells can be 0 (dead) or 1 (alive). Rules are simplified for demo purposes.
 */
class SimpleBoard(width: Int, height: Int) extends Board {
  private var grid = Array.fill(width, height)(0)
  private var nextGrid = Array.fill(width, height)(0)
  @volatile private var converged = false
  @volatile private var generation = 0
  private val maxGenerations = 5

  def getMaxX: Int = width
  def getMaxY: Int = height

  def getValue(x: Int, y: Int): Int = synchronized {
    grid(x)(y)
  }

  def setNewValue(x: Int, y: Int, value: Int): Unit = synchronized {
    nextGrid(x)(y) = value
  }

  def commitNewValues(): Unit = synchronized {
    var changed = false
    for (x <- 0 until width; y <- 0 until height) {
      if (grid(x)(y) != nextGrid(x)(y)) changed = true
      grid(x)(y) = nextGrid(x)(y)
    }
    generation += 1
    converged = !changed || generation >= maxGenerations
    println(s"[CellularAutomata] Generation $generation committed. Converged: $converged")
  }

  def hasConverged: Boolean = synchronized {
    converged
  }

  /**
   * Split the board into `numPartitions` vertical slices.
   * Each worker gets a subboard spanning certain x-ranges.
   */
  def getSubBoard(numPartitions: Int, partitionIdx: Int): Board = {
    val xStart = (width * partitionIdx) / numPartitions
    val xEnd = (width * (partitionIdx + 1)) / numPartitions
    new SubBoard(this, xStart, xEnd)
  }

  def waitForConvergence(): Unit = {
    @tailrec
    def loop(): Unit = {
      if (!hasConverged) {
        Thread.sleep(100)
        loop()
      }
    }
    loop()
  }

  /**
   * Initialize the board with a random pattern (glider, blinker, etc.)
   */
  def initializeRandom(): Unit = synchronized {
    val rand = scala.util.Random
    for (x <- 0 until width; y <- 0 until height) {
      grid(x)(y) = if (rand.nextDouble() < 0.3) 1 else 0
    }
  }

  def printBoard(): Unit = synchronized {
    println(s"\nGeneration $generation:")
    for (y <- 0 until Math.min(height, 10); x <- 0 until Math.min(width, 10)) {
      print(if (grid(x)(y) == 1) "█" else "·")
    }
    println()
  }

  /**
   * Simplified neighbor counting for cellular automata rule.
   */
  private def countNeighbors(x: Int, y: Int): Int = {
    var count = 0
    for (dx <- -1 to 1; dy <- -1 to 1 if !(dx == 0 && dy == 0)) {
      val nx = (x + dx + width) % width
      val ny = (y + dy + height) % height
      if (grid(nx)(ny) == 1) count += 1
    }
    count
  }

  /**
   * Conway's Game of Life rules:
   *  - Live cell with 2-3 neighbors survives
   *  - Dead cell with 3 neighbors becomes alive
   *  - All others die or stay dead
   */
  def computeValue(x: Int, y: Int): Int = synchronized {
    val neighbors = countNeighbors(x, y)
    val alive = grid(x)(y) == 1
    if (alive && (neighbors == 2 || neighbors == 3)) 1
    else if (!alive && neighbors == 3) 1
    else 0
  }
}

/**
 * A view into a portion of the main board (for worker threads).
 */
private class SubBoard(mainBoard: SimpleBoard, xStart: Int, xEnd: Int) extends Board {
  def getMaxX: Int = xEnd - xStart
  def getMaxY: Int = mainBoard.getMaxY
  def getValue(x: Int, y: Int): Int = mainBoard.getValue(xStart + x, y)
  def setNewValue(x: Int, y: Int, value: Int): Unit = mainBoard.setNewValue(xStart + x, y, value)
  def commitNewValues(): Unit = mainBoard.commitNewValues()
  def hasConverged: Boolean = mainBoard.hasConverged
  def getSubBoard(numPartitions: Int, partitionIdx: Int): Board = this
  def waitForConvergence(): Unit = mainBoard.waitForConvergence()

  def computeValue(x: Int, y: Int): Int = mainBoard.computeValue(xStart + x, y)
}

/**
 * Main cellular automata coordinator using CyclicBarrier.
 */
class CellularAutomata(mainBoard: SimpleBoard) {
  private val board = mainBoard
  private val numWorkers = Runtime.getRuntime.availableProcessors
  private val barrier = new CyclicBarrier(numWorkers, new Runnable {
    def run(): Unit = {
      board.commitNewValues()
    }
  })
  private val workers = Array.fill(numWorkers)(
    new Worker(board.getSubBoard(numWorkers, 0))
  )

  for (i <- 0 until numWorkers) {
    workers(i) = new Worker(board.getSubBoard(numWorkers, i))
  }

  /**
   * Inner Worker class: each thread processes its subboard partition.
   */
  private class Worker(subBoard: Board) extends Runnable {
    def run(): Unit = {
      try {
        while (!subBoard.hasConverged) {
          // Compute new values for this worker's portion of the board
          for (x <- 0 until subBoard.getMaxX; y <- 0 until subBoard.getMaxY) {
            val newVal = subBoard.asInstanceOf[SubBoard].computeValue(x, y)
            subBoard.setNewValue(x, y, newVal)
          }
          // Wait for all workers to finish, then commit
          barrier.await()
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          println("[CellularAutomata] Worker interrupted")
        case _: BrokenBarrierException =>
          println("[CellularAutomata] Worker encountered broken barrier")
      }
    }
  }

  def start(): Unit = {
    println(s"[CellularAutomata] Starting with $numWorkers worker threads")
    for (i <- 0 until numWorkers) {
      new Thread(workers(i)).start()
    }
    board.waitForConvergence()
    println("[CellularAutomata] Simulation converged, all workers finished")
  }
}

/**
 * Demo: run a small cellular automata simulation.
 */
object CellularAutomataDemo {
  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("Cellular Automata with CyclicBarrier")
    println("=" * 70)

    val boardSize = 20
    val board = new SimpleBoard(boardSize, boardSize)
    board.initializeRandom()
    board.printBoard()

    val automata = new CellularAutomata(board)
    automata.start()

    println("\nFinal board state:")
    board.printBoard()
    println("\n" + "=" * 70)
  }
}

