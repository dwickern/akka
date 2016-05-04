/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import scala.concurrent.duration._
import akka.dispatch.sysmsg.SystemMessage
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.remote.artery.Transport.InboundEnvelope
import akka.actor.Address
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.AsyncCallback
import akka.Done
import scala.concurrent.Future
import scala.concurrent.Promise
import akka.remote.EndpointManager.Send
import akka.remote.artery.Transport.InboundEnvelope
import akka.actor.ActorRef

/**
 * INTERNAL API
 */
private[akka] object SystemMessageDelivery {
  final case class SystemMessageEnvelope(message: SystemMessage, seqNo: Long, ackReplyTo: ActorRef)
  sealed trait SystemMessageReply
  final case class Ack(seq: Long, from: Address) extends SystemMessageReply
  final case class Nack(seq: Long, from: Address) extends SystemMessageReply
}

/**
 * INTERNAL API
 */
private[akka] class SystemMessageDelivery(replyJunction: SystemMessageReplyJunction.Junction,
                                          localAddress: Address, remoteAddress: Address, ackRecipient: ActorRef)
  extends GraphStage[FlowShape[Send, Send]] {
  import SystemMessageDelivery._
  import SystemMessageReplyJunction._

  val in: Inlet[Send] = Inlet("in")
  val out: Outlet[Send] = Outlet("out")
  override val shape: FlowShape[Send, Send] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      var registered = false
      var seqNo = 0L // sequence number for the first message will be 1

      val ackCallback = getAsyncCallback[SystemMessageReply] { env ⇒
        println(s"# [$localAddress] SystemMessageDelivery got reply $env") // FIXME
        // FIXME if Ack then remove from resend buffer
        //       if Nack then redeliver
      }

      override def preStart(): Unit = {
        def filter(env: InboundEnvelope): Boolean =
          env.message match {
            case Ack(_, from) if from == remoteAddress  ⇒ true
            case Nack(_, from) if from == remoteAddress ⇒ true
            case _                                      ⇒ false
          }

        implicit val ec = materializer.executionContext
        replyJunction.addReplyInterest(filter, ackCallback).foreach { _ ⇒
          getAsyncCallback[Unit] { _ ⇒
            registered = true
            if (isAvailable(out))
              pull(in) // onPull from downstream already called
          }.invoke(())
        }

        // FIXME periodic redelivery of resend buffer
      }

      // InHandler
      override def onPush(): Unit = {
        val sendMsg = grab(in) match {
          case s @ Send(msg: SystemMessage, _, _, _) ⇒
            seqNo += 1
            // FIXME store in resend buffer
            s.copy(message = SystemMessageEnvelope(msg, seqNo, ackRecipient))
          case s ⇒ s
        }

        push(out, sendMsg)
      }

      // OutHandler
      override def onPull(): Unit = {
        if (registered)
          pull(in) // otherwise it will be pulled after replyJunction.addReplyInterest
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
private[akka] class SystemMessageAcker(localAddress: Address) extends GraphStage[FlowShape[InboundEnvelope, InboundEnvelope]] {
  import SystemMessageDelivery._

  val in: Inlet[InboundEnvelope] = Inlet("in")
  val out: Outlet[InboundEnvelope] = Outlet("out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      var seqNo = 1L

      // InHandler
      override def onPush(): Unit = {
        grab(in) match {
          case env @ InboundEnvelope(_, _, sysEnv @ SystemMessageEnvelope(_, n, ackReplyTo), _) ⇒
            if (n == seqNo) {
              println(s"# [$localAddress] SystemMessageAcker sending ack $n ") // FIXME
              ackReplyTo.tell(Ack(n, localAddress), ActorRef.noSender)
              seqNo += 1
            } else if (n < seqNo) {
              println(s"# [$localAddress] SystemMessageAcker discarding duplicate $env") // FIXME
            } else {
              println(s"# [$localAddress] SystemMessageAcker sending nack $n > $seqNo ") // FIXME
              ackReplyTo.tell(Nack(seqNo, localAddress), ActorRef.noSender)
            }
            val unwrapped = env.copy(message = sysEnv.message)
            push(out, unwrapped)
          case env ⇒
            // messages that don't need acking
            push(out, env)
        }

      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

object SystemMessageReplyJunction {
  import SystemMessageDelivery._

  trait Junction {
    def addReplyInterest(filter: InboundEnvelope ⇒ Boolean, replyCallback: AsyncCallback[SystemMessageReply]): Future[Done]
  }
}

class SystemMessageReplyJunction
  extends GraphStageWithMaterializedValue[FlowShape[InboundEnvelope, InboundEnvelope], SystemMessageReplyJunction.Junction] {
  import SystemMessageReplyJunction._
  import SystemMessageDelivery._

  val in: Inlet[InboundEnvelope] = Inlet("in")
  val out: Outlet[InboundEnvelope] = Outlet("out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new GraphStageLogic(shape) with InHandler with OutHandler with Junction {

      private var replyHandlers: Vector[(InboundEnvelope ⇒ Boolean, AsyncCallback[SystemMessageReply])] = Vector.empty

      // InHandler
      override def onPush(): Unit = {
        grab(in) match {
          case env @ InboundEnvelope(_, _, reply: SystemMessageReply, _) ⇒
            replyHandlers.foreach {
              case (f, callback) ⇒
                if (f(env))
                  callback.invoke(reply)
            }
            pull(in)
          case env ⇒
            push(out, env)
        }
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      override def addReplyInterest(filter: InboundEnvelope ⇒ Boolean, replyCallback: AsyncCallback[SystemMessageReply]): Future[Done] = {
        val p = Promise[Done]()
        getAsyncCallback[Unit](_ ⇒ {
          replyHandlers :+= (filter -> replyCallback)
          p.success(Done)
        }).invoke(())
        p.future
      }

      setHandlers(in, out, this)
    }
    (logic, logic)
  }
}
