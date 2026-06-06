import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

class JvmShutdownHookVsFinalizerDemoTest extends AnyFunSuite {

  test("JVM waits for a non-daemon worker after main returns") {
    val output = runDemoInForkedJvm("main-finishes-first")

    assertInOrder(
      output,
      "[demo-1][main] returning now while worker is still running",
      "[demo-1][worker] still running after main announced its return",
      "[demo-1][worker] completed",
      "[demo-1][shutdown-hook][demo-1-shutdown-hook] started during JVM shutdown"
    )
  }

  test("shutdown hook runs on its registered thread after the worker completes") {
    val output = runDemoInForkedJvm("main-finishes-first")

    assertInOrder(
      output,
      "[demo-1][worker] completed",
      "[demo-1][shutdown-hook][demo-1-shutdown-hook] started during JVM shutdown"
    )
  }

  test("shutdown hook starts after worker and main have both completed") {
    val output = runDemoInForkedJvm("worker-finishes-first")

    assertInOrder(
      output,
      "[demo-2][worker] completed",
      "[demo-2][main] worker has completed",
      "[demo-2][main] returning now",
      "[demo-2][shutdown-hook][demo-2-shutdown-hook] started during JVM shutdown"
    )
  }

  private def assertInOrder(output: String, expectedLines: String*): Unit = {
    expectedLines.foldLeft(-1) { (previousIndex, line) =>
      val currentIndex = output.indexOf(line, previousIndex + 1)
      assert(currentIndex >= 0, s"missing line '$line' in output:\n$output")
      currentIndex
    }
  }

  private def runDemoInForkedJvm(mode: String): String = {
    val javaBin = System.getProperty("java.home") + "/bin/java"
    val classpath = System.getProperty("java.class.path")
    val processBuilder = new ProcessBuilder(
      javaBin,
      "-cp",
      classpath,
      "JvmShutdownHookVsFinalizerDemo",
      mode
    )
    processBuilder.redirectErrorStream(true)

    val process = processBuilder.start()
    val output = new ByteArrayOutputStream()

    val reader = new Thread(new Runnable {
      override def run(): Unit = {
        val input = process.getInputStream
        val buffer = new Array[Byte](8192)
        var read = input.read(buffer)

        while (read != -1) {
          output.write(buffer, 0, read)
          read = input.read(buffer)
        }
      }
    }, s"$mode-output-reader")

    reader.setDaemon(true)
    reader.start()

    val finished = process.waitFor(30, TimeUnit.SECONDS)
    if (!finished) {
      process.destroyForcibly()
      reader.join(5000)
      fail(s"$mode demo did not exit within 30 seconds")
    }

    reader.join(5000)
    if (reader.isAlive)
      fail(s"failed to drain output from $mode demo")

    val exitCode = process.exitValue()
    val text = new String(output.toByteArray, StandardCharsets.UTF_8)

    assert(exitCode == 0, s"$mode demo exited with code $exitCode\n$text")
    text
  }
}
