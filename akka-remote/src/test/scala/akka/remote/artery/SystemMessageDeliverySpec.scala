/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import scala.concurrent.duration._

import akka.actor.ActorIdentity
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.actor.PoisonPill
import akka.actor.RootActorPath
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.SocketUtil
import akka.testkit.TestActors
import com.typesafe.config.ConfigFactory

object SystemMessageDeliverySpec {

  val Seq(portA, portB) = SocketUtil.temporaryServerAddresses(2, "localhost", udp = true).map(_.getPort)

  val commonConfig = ConfigFactory.parseString(s"""
     akka {
       actor.provider = "akka.remote.RemoteActorRefProvider"
       remote.artery.enabled = on
       remote.artery.hostname = localhost
       remote.artery.port = $portA
     }
  """)

  val configB = ConfigFactory.parseString(s"akka.remote.artery.port = $portB")
    .withFallback(commonConfig)

}

class SystemMessageDeliverySpec extends AkkaSpec(SystemMessageDeliverySpec.commonConfig) with ImplicitSender {

  val systemB = ActorSystem("systemB", SystemMessageDeliverySpec.configB)
  val addressB = systemB.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  val rootB = RootActorPath(addressB)

  override def afterTermination(): Unit = shutdown(systemB)

  "System messages" must {

    "be delivered" in {
      val actorOnSystemB = systemB.actorOf(TestActors.echoActorProps, "echo")

      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      watch(remoteRef)
      remoteRef ! PoisonPill
      expectTerminated(remoteRef)
    }

  }

}
