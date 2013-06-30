package net.surguy.queue

import java.io.{File, InputStream}
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.model._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.AmazonS3Client
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.Random

/**
 * Uploads a directory of local files to a queue and an associated file store.
 */
class QueuePreparer(val store: Store, val queue: Queue[_]) {
  def transferToStore(localDirectory: File) { localDirectory.listFiles().foreach( f => store.addToStore(f.getName, f) ) }
  def populateQueue() { store.listContents().foreach( queue.add ) }
}

/**
 * Processes each of the items in a queue, retrieving each from a store by its identifier and processing it.
 */
class QueueConsumer[U](val store: Store, val queue: Queue[U]) {
  def processNextIdentifier[T](fn: ((String, InputStream) => T)) = queue.nextIdentifier().map{ m =>
    val result = fn(m.identifier, store.retrieveFromStore(m.identifier))
    queue.removeMessage(m)
    result
  }
}

abstract class Store {
  def listContents(): Seq[String]
  def addToStore(identifier: String, file: File)
  def retrieveFromStore(identifier:String): InputStream
  def clearAll()
}

abstract class Queue[T] {
  def add(identifier: String)
  def nextIdentifier(): Option[MessageWrapper[T]]
  def removeMessage(msg: MessageWrapper[T])
  def clearAll()
}

class AmazonS3Store(val bucketName: String) extends Store {
  private val client = {
    val s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider())
    s3.setRegion(Region.getRegion(Regions.US_WEST_2))
    if (! s3.listBuckets().map( _.getName ).contains(bucketName)) s3.createBucket(bucketName)
    s3
  }

  def listContents() = {
    val contents = new ListBuffer[String]
    var objects = client.listObjects(new ListObjectsRequest().withBucketName(bucketName))
    contents.addAll(objects.getObjectSummaries.map( _.getKey ))
    while (objects.isTruncated) {
      objects = client.listNextBatchOfObjects(objects)
      contents.addAll(objects.getObjectSummaries.map( _.getKey ))
    }
    contents.toList
  }
  def addToStore(identifier: String, file: File) { client.putObject(new PutObjectRequest(bucketName, identifier, file)) }
  def retrieveFromStore(identifier: String) = client.getObject(new GetObjectRequest(bucketName, identifier)).getObjectContent
  def clearAll() { client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys( listContents(): _*  )) }
}

class AmazonSqsQueue(val queueName: String) extends Queue[String] {
  private val client = {
    val sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider())
    sqs.setRegion(Region.getRegion(Regions.US_WEST_2))
    sqs
  }
  private val queueUrl = {
    val createRequest = new CreateQueueRequest(queueName)
    // This determines how long a message is hidden from other requesters after it's been received (seconds)
    // It should be similar to the expected time taken to process a message
    createRequest.setAttributes(Map(QueueAttributeName.VisibilityTimeout.name() -> "120"))
    client.createQueue(createRequest).getQueueUrl
  }

  def add(identifier: String) { client.sendMessage(new SendMessageRequest(queueUrl, identifier)) }
  def nextIdentifier() = {
    val receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
    receiveMessageRequest.setMaxNumberOfMessages(1) // How many messages to retrieve at once
    receiveMessageRequest.setWaitTimeSeconds(20) // How long to block waiting for a message to become available (20 is maximum)
    val messages = client.receiveMessage(receiveMessageRequest).getMessages
    messages.headOption.map( m => new MessageWrapper(m.getBody, m.getReceiptHandle) )
  }
  def removeMessage(msg: MessageWrapper[String]) { client.deleteMessage(new DeleteMessageRequest(queueUrl, msg.messageHandle)) }
  def clearAll() {
    def getMessages = {
      val receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
      receiveMessageRequest.setMaxNumberOfMessages(10)
      receiveMessageRequest.setWaitTimeSeconds(0)
      client.receiveMessage(receiveMessageRequest).getMessages
    }
    var messages = getMessages
    val rnd = new Random()
    while (! messages.isEmpty) {
      val deleteRequests = messages.map(m => new DeleteMessageBatchRequestEntry(""+rnd.nextLong()+System.nanoTime(), m.getReceiptHandle))
      client.deleteMessageBatch(new DeleteMessageBatchRequest(queueUrl, deleteRequests))
      messages = getMessages
    }
  }
}
case class MessageWrapper[T](identifier: String, messageHandle: T)

