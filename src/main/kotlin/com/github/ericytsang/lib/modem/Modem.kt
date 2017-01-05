package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.regulatedstream.RegulatedOutputStream
import com.github.ericytsang.lib.abstractstream.AbstractInputStream
import com.github.ericytsang.lib.abstractstream.AbstractOutputStream
import com.github.ericytsang.lib.net.connection.Connection
import com.github.ericytsang.lib.net.host.Client
import com.github.ericytsang.lib.net.host.Server
import com.github.ericytsang.lib.simplepipestream.SimplePipedInputStream
import com.github.ericytsang.lib.simplepipestream.SimplePipedOutputStream
import java.io.EOFException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.util.LinkedHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

class Modem(val multiplexedConnection:Connection):Client<Unit>,Server
{
    //
    // Server & Client
    //

    override fun accept():Connection = synchronized(inboundConnectionRequestsIs)
    {
        val connectRequest = inboundConnectionRequestsIs.readObject() as Message.Connect
        val connectionLocalPort = makeNewConnectionAndReturnPort()
        val connection = connectionsByLocalPort[connectionLocalPort]!!
        connection.remotePort = connectRequest.srcPort
        send(Message.Accept(connectionLocalPort,connectRequest.srcPort))
        send(Message.Ack(connection.remotePort,connection.inputStreamOutputStream.bufferSize))
        return connection
    }

    override fun connect(remoteAddress:Unit):Connection = synchronized(outboundConnectionRequests)
    {
        val connectionLocalPort = makeNewConnectionAndReturnPort()
        val recipient = outboundConnectionRequests.expectMail(connectionLocalPort)
        send(Message.Connect(connectionLocalPort))
        recipient.awaitMail()
        val connection = connectionsByLocalPort[connectionLocalPort]!!
        send(Message.Ack(connection.remotePort,connection.inputStreamOutputStream.bufferSize))
        return connection
    }

    override fun close()
    {
        // todo: communicate to remote modem that this modem is closing
        multiplexedConnection.close()
        accepter.join()
    }

    //
    // private helper members
    //

    /**
     * objects transmitted through the [multiplexedConnection].
     */
    private sealed class Message:Serializable
    {
        class Connect(val srcPort:Int):Message(),Serializable
        class Accept(val srcPort:Int,val dstPort:Int):Message(),Serializable

        /**
         * promise remote party that local party will no longer send any
         * [Message] objects to [dstPort] for this [Connection].
         */
        class Eof(val dstPort:Int):Message(),Serializable

        /**
         * request remote party to no longer send [Data] messages from [dstPort]
         * for this [Connection].
         */
        class RequestEof(val dstPort:Int):Message(),Serializable
        class Data(val dstPort:Int,val payload:ByteArray):Message(),Serializable
        class Ack(val dstPort:Int,val bytesRead:Int):Message(),Serializable

        /**
         * used to acknowledge the remote party that the [Eof] message has been
         * received.
         */
        class AckEof(val dstPort:Int):Message(),Serializable
    }

    /**
     * contains inbound connection requests that have yet to be accepted.
     */
    private val inboundConnectionRequestsIs:ObjectInputStream
    private val inboundConnectionRequestsOs:ObjectOutputStream

    init
    {
        val o = SimplePipedOutputStream()
        val i = SimplePipedInputStream(o)
        inboundConnectionRequestsOs = ObjectOutputStream(o)
        inboundConnectionRequestsIs = ObjectInputStream(i)
    }

    /**
     * keeps track of outbound connection requests that have yet to be accepted.
     */
    private val outboundConnectionRequests = Mailbox<Int,Unit>()

    /**
     * maps local ports to connection objects.
     */
    private val connectionsByLocalPort = LinkedHashMap<Int,SimpleConnection>()

    private val accepter = thread()
    {
        val multiplexedIs = ObjectInputStream(multiplexedConnection.inputStream)
        while (!Thread.interrupted())
        {
            val message:Message = try
            {
                multiplexedIs.readObject() as Message
            }
            catch (ex:EOFException)
            {
                connectionsByLocalPort.values.forEach {
                    it.inputStreamOutputStream.flush()
                    it.inputStreamOutputStream.close()
                }
                break
            }
            synchronized(connectionsByLocalPort)
            {
                when (message)
                {
                    is Message.Connect -> inboundConnectionRequestsOs.writeObject(message)
                    is Message.Accept ->
                    {
                        connectionsByLocalPort[message.dstPort]!!.remotePort = message.srcPort
                        outboundConnectionRequests.putMail(message.dstPort,Unit)
                    }
                    is Message.Eof ->
                    {
                        val connection = connectionsByLocalPort[message.dstPort]!!
                        connection.inputStreamOutputStream.close()
                        connection.remoteOutputStreamIsClosed = true
                        connection.removeConnectionIfBothStreamsAreClosed()
                        send(Message.AckEof(connection.remotePort))
                    }
                    is Message.AckEof ->
                    {
                        connectionsByLocalPort[message.dstPort]!!.localOutputStreamIsClosed = true
                        connectionsByLocalPort[message.dstPort]!!.removeConnectionIfBothStreamsAreClosed()
                    }
                    is Message.RequestEof -> connectionsByLocalPort[message.dstPort]!!.outputStream.close()
                    is Message.Data -> connectionsByLocalPort[message.dstPort]!!.inputStreamOutputStream.write(message.payload)
                    is Message.Ack -> connectionsByLocalPort[message.dstPort]!!.outputStream.permit(message.bytesRead)
                }.apply {}
            }
        }
        inboundConnectionRequestsOs.close()
        outboundConnectionRequests.close()
    }

    private fun send(message:Message)
    {
        sender.send(message)
    }

    private val sender = object
    {
        /**
         * monitor that must be held before writing to [multiplexedOs].
         */
        private val multiplexedConnectionAccess = ReentrantLock()

        private val multiplexedOs = ObjectOutputStream(multiplexedConnection.outputStream)

        fun send(message:Message) = multiplexedConnectionAccess.withLock()
        {
            multiplexedOs.writeObject(message)
            multiplexedOs.flush()
        }
    }

    /**
     * creates a new [SimpleConnection] object and puts it in a unused port in
     * the [connectionsByLocalPort] map.
     */
    private fun makeNewConnectionAndReturnPort():Int = synchronized(connectionsByLocalPort)
    {
        val unusedPort = (Int.MIN_VALUE..Int.MAX_VALUE).find {it !in connectionsByLocalPort.keys} ?: throw RuntimeException("failed to allocate port for new connection")
        connectionsByLocalPort[unusedPort] = SimpleConnection(unusedPort)
        unusedPort
    }

    private inner class SimpleConnection(val localPort:Int):Connection
    {
        /**
         * set once the connection has been accepted.
         */
        private var _remotePort:Int? = null
        var remotePort:Int
            set(value)
            {
                if (_remotePort != null) throw IllegalStateException("already set")
                _remotePort = value
            }
            get() = _remotePort ?: throw IllegalStateException("not set yet")

        /**
         * bytes written to this stream can be read from [inputStream].
         */
        val inputStreamOutputStream = SimplePipedOutputStream()

        var remoteOutputStreamIsClosed = false

        /**
         * stream that clients of the [Modem] object read from.
         */
        override val inputStream = object:AbstractInputStream()
        {
            val stream = SimplePipedInputStream(inputStreamOutputStream)
            private var closeCalled = false
            override fun doClose() = synchronized(connectionsByLocalPort)
            {
                if (!closeCalled)
                {
                    closeCalled = true
                    if (!remoteOutputStreamIsClosed)
                    {
                        send(Message.RequestEof(remotePort))
                        doNothing()
                    }
                }
            }
            override fun doRead(b:ByteArray,off:Int,len:Int):Int
            {
                val read = stream.read(b,off,len)
                synchronized(connectionsByLocalPort)
                {
                    if (!remoteOutputStreamIsClosed && read >= 0)
                    {
                        send(Message.Ack(remotePort,read))
                    }
                }
                return read
            }
        }

        var localOutputStreamIsClosed = false

        /**
         * stream that clients of the [Modem] object write to.
         */
        override val outputStream = object:AbstractOutputStream()
        {
            private var closeCalled = false
            override fun doClose() = synchronized(this)
            {
                if (!closeCalled)
                {
                    closeCalled = true
                    send(Message.Eof(remotePort))
                    setClosed()
                }
            }
            override fun doWrite(b:ByteArray,off:Int,len:Int)
            {
                send(Message.Data(remotePort,b.sliceArray(off..off+len-1)))
            }
        }.let(::RegulatedOutputStream)

        fun removeConnectionIfBothStreamsAreClosed()
        {
            synchronized(connectionsByLocalPort)
            {
                if (remoteOutputStreamIsClosed && localOutputStreamIsClosed)
                {
                    if (connectionsByLocalPort[localPort] == this)
                    {
                        connectionsByLocalPort.remove(localPort)
                    }
                }
            }
        }

        override fun close()
        {
            inputStream.close()
            outputStream.close()
        }
    }
}
