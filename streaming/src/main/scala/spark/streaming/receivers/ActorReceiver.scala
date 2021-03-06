package spark.streaming.receivers

import akka.actor.{ Actor, PoisonPill, Props, SupervisorStrategy }
import akka.actor.{ actorRef2Scala, ActorRef }
import akka.actor.{ PossiblyHarmful, OneForOneStrategy }
import akka.actor.SupervisorStrategy._

import scala.concurrent.duration._
import scala.reflect.ClassTag

import spark.storage.StorageLevel
import spark.streaming.dstream.NetworkReceiver

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

/** A helper with set of defaults for supervisor strategy */
object ReceiverSupervisorStrategy {

  val defaultStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException ⇒ Restart
    case _: Exception ⇒ Escalate
  }
}

/**
 * A receiver trait to be mixed in with your Actor to gain access to
 * pushBlock API.
 *
 * @example {{{
 *  class MyActor extends Actor with Receiver{
 *      def receive {
 *          case anything :String ⇒ pushBlock(anything)
 *      }
 *  }
 *  //Can be plugged in actorStream as follows
 *  ssc.actorStream[String](Props(new MyActor),"MyActorReceiver")
 *
 * }}}
 *
 * @note An important point to note:
 *       Since Actor may exist outside the spark framework, It is thus user's responsibility
 *       to ensure the type safety, i.e parametrized type of push block and InputDStream
 *       should be same.
 *
 */
trait Receiver { self: Actor ⇒
  def pushBlock[T: ClassTag](iter: Iterator[T]) {
    context.parent ! Data(iter)
  }

  def pushBlock[T: ClassTag](data: T) {
    context.parent ! Data(data)
  }

}

/**
 * Statistics for querying the supervisor about state of workers
 */
case class Statistics(numberOfMsgs: Int,
  numberOfWorkers: Int,
  numberOfHiccups: Int,
  otherInfo: String)

/** Case class to receive data sent by child actors */
private[streaming] case class Data[T: ClassTag](data: T)

/**
 * Provides Actors as receivers for receiving stream.
 *
 * As Actors can also be used to receive data from almost any stream source.
 * A nice set of abstraction(s) for actors as receivers is already provided for
 * a few general cases. It is thus exposed as an API where user may come with
 * his own Actor to run as receiver for Spark Streaming input source.
 *
 * This starts a supervisor actor which starts workers and also provides
 *  [http://doc.akka.io/docs/akka/2.0.5/scala/fault-tolerance.html fault-tolerance].
 *
 *  Here's a way to start more supervisor/workers as its children.
 *
 * @example {{{
 *  context.parent ! Props(new Supervisor)
 * }}} OR {{{
 *  context.parent ! Props(new Worker,"Worker")
 * }}}
 *
 *
 */
private[streaming] class ActorReceiver[T: ClassTag](
  props: Props,
  name: String,
  storageLevel: StorageLevel,
  receiverSupervisorStrategy: SupervisorStrategy)
  extends NetworkReceiver[T] {

  protected lazy val blocksGenerator: BlockGenerator =
    new BlockGenerator(storageLevel)

  protected lazy val supervisor = env.actorSystem.actorOf(Props(new Supervisor),
    "Supervisor" + streamId)

  private class Supervisor extends Actor {

    override val supervisorStrategy = receiverSupervisorStrategy
    val worker = context.actorOf(props, name)
    logInfo("Started receiver worker at:" + worker.path)

    val n: AtomicInteger = new AtomicInteger(0)
    val hiccups: AtomicInteger = new AtomicInteger(0)

    def receive = {

      case Data(iter: Iterator[_]) ⇒ pushBlock(iter.asInstanceOf[Iterator[T]])

      case Data(msg) ⇒
        blocksGenerator += msg.asInstanceOf[T]
        n.incrementAndGet

      case props: Props ⇒
        val worker = context.actorOf(props)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case (props: Props, name: String) ⇒
        val worker = context.actorOf(props, name)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case _: PossiblyHarmful => hiccups.incrementAndGet()

      case _: Statistics ⇒
        val workers = context.children
        sender ! Statistics(n.get, workers.size, hiccups.get, workers.mkString("\n"))

    }
  }

  protected def pushBlock(iter: Iterator[T]) {
    val buffer = new ArrayBuffer[T]
    buffer ++= iter
    pushBlock("block-" + streamId + "-" + System.nanoTime(), buffer, null, storageLevel)
  }

  protected def onStart() = {
    blocksGenerator.start()
    supervisor
    logInfo("Supervision tree for receivers initialized at:" + supervisor.path)
  }

  protected def onStop() = {
    supervisor ! PoisonPill
  }

}
