package net.surguy.queue

import org.specs2.mutable.Specification
import java.io.{InputStream, File}
import com.google.common.io.{ByteStreams, Files}
import com.google.common.base.Charsets

class QueueSpec extends Specification {
  sequential
  val store = new AmazonS3Store("eae5c290-e0fd-11e2-a28f-0800200c9a66-queuetest")
  val queue = new AmazonSqsQueue("queuetest")
  val preparer = new QueuePreparer(store, queue)
  val consumer = new QueueConsumer[String](store, queue)
  val uniqueId = ""+System.nanoTime()

  def createFile(parentDir: Option[File] = None) = {
    val tempFile = if (parentDir.isDefined) File.createTempFile("prefix", ".txt", parentDir.get) else  File.createTempFile("prefix", ".txt")
    tempFile.deleteOnExit()
    Files.write("Test text "+uniqueId, tempFile, Charsets.UTF_8)
    tempFile
  }

  "accessing the store" should {
    "support storing and retrieving items" in {
      store.clearAll()
      store.listContents() must beEmpty
      store.addToStore(uniqueId, createFile())
      store.listContents() must beEqualTo(List(uniqueId))
      new String(ByteStreams.toByteArray(store.retrieveFromStore(uniqueId)), Charsets.UTF_8) must contain(uniqueId)
    }
  }

  "accessing the queue" should {
    "support adding to and removing from the queue" in {
      queue.clearAll()
      queue.countMessages() mustEqual 0
      queue.nextIdentifier() must beNone
      queue.add(uniqueId)
      queue.countMessages() mustEqual 1
      val msg = queue.nextIdentifier()
      msg.map(_.identifier) must beEqualTo(Some(uniqueId))
      queue.nextIdentifier() must beNone
      queue.countMessages() mustEqual 1
      queue.removeMessage(msg.get)
      queue.countMessages() mustEqual 0
    }
  }

  "processing files" should {
    "upload files for testing" in {
      store.clearAll()
      queue.clearAll()

      val tempDir = Files.createTempDir()
      val f1 = createFile(Some(tempDir))
      val f2 = createFile(Some(tempDir))
      preparer.transferToStore(tempDir)
      preparer.populateQueue()
      store.listContents().toSet must beEqualTo(Set(f1.getName, f2.getName))
      def processFn(identifier: String, input: InputStream) = new String(ByteStreams.toByteArray(input), Charsets.UTF_8)
      consumer.processNextIdentifier(processFn) must beEqualTo(Some("Test text "+uniqueId))
      consumer.processNextIdentifier(processFn) must beEqualTo(Some("Test text "+uniqueId))
    }
  }
}

