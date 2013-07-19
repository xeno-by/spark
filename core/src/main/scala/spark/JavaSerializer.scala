package spark

import java.io._
import java.nio.ByteBuffer

import serializer.{Serializer, SerializerInstance, DeserializationStream, SerializationStream}
import spark.util.ByteBufferInputStream

object JavaTimer {
  var value = new java.util.concurrent.atomic.AtomicLong
}

private[spark] class JavaSerializationStream(out: OutputStream) extends SerializationStream with Logging {
  val objOut = new ObjectOutputStream(out)
  def writeObject[T](t: T): SerializationStream = {
    // import java.io.StringWriter;
    // import java.io.PrintWriter;
    // val sw = new StringWriter();
    // new Throwable().printStackTrace(new PrintWriter(sw));
    // logInfo("Current stack trace is:\n\t" + sw.toString());
    val start = System.currentTimeMillis()
    objOut.writeObject(t)
    val end = System.currentTimeMillis()
    JavaTimer.value.addAndGet(end - start)
    this
  }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
extends DeserializationStream {
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T](): T = {
    val start = System.currentTimeMillis()
    val result = objIn.readObject().asInstanceOf[T]
    val end = System.currentTimeMillis()
    JavaTimer.value.addAndGet(end - start)
    result
  }
  def close() { objIn.close() }
}

private[spark] class JavaSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject().asInstanceOf[T]
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, Thread.currentThread.getContextClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * A Spark serializer that uses Java's built-in serialization.
 */
class JavaSerializer extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance
}
