package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.regulatedstream.RegulatedOutputStream
import com.github.ericytsang.lib.abstractstream.AbstractInputStream
import com.github.ericytsang.lib.abstractstream.AbstractOutputStream
import com.github.ericytsang.lib.net.connection.Connection
import com.github.ericytsang.lib.net.host.Client
import com.github.ericytsang.lib.net.host.Server
import com.github.ericytsang.lib.simplepipestream.SimplePipedInputStream
import com.github.ericytsang.lib.simplepipestream.SimplePipedOutputStream
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.io.Serializable
import java.net.ConnectException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Modem(val multiplexedConnection:Connection):Client<Unit>,Server
{
    companion object
    {
        const val RECV_WINDOW_SIZE_PER_CONNECTION = 4096
    }

    override fun connect(remoteAddress:Unit):Connection
    {
        val localPort = makeNewConnectionOnLocalPort()
        val connection = connectionsByLocalPort[localPort]!!
        sender.send(Message.Connect(localPort))
        connection.connectLatch.await()
        if (connection.state !is SimpleConnection.Connected) throw ConnectException()
        return connection
    }

    override fun accept():Connection
    {
        val inboundConnect = inboundConnectsObjI.readObject() as Message.Connect
        val localPort = makeNewConnectionOnLocalPort()
        val connection = connectionsByLocalPort[localPort]!!
        connection.receive(Message.Accept(inboundConnect.srcPort,localPort))
        sender.send(Message.Accept(localPort,inboundConnect.srcPort))
        return connection
    }

    override fun close()
    {
        try
        {
            inboundConnectsObjO.close()
            multiplexedConnection.close()
        }
        catch (ex:Exception)
        {
            // ignore
        }
        synchronized(connectionsByLocalPort)
        {
            connectionsByLocalPort.values.toList().forEach(SimpleConnection::close)
            connectionsByLocalPort.clear()
        }
        if (Thread.currentThread() != reader)
        {
            reader.join()
        }
    }

    private val inboundConnectsObjO:ObjectOutputStream
    private val inboundConnectsObjI:ObjectInputStream

    init
    {
        val pipeO = SimplePipedOutputStream()
        val pipeI = SimplePipedInputStream(pipeO)
        inboundConnectsObjO = ObjectOutputStream(pipeO)
        inboundConnectsObjI = ObjectInputStream(pipeI)
    }

    private val connectionsByLocalPort = linkedMapOf<Int,SimpleConnection>()

    private fun makeNewConnectionOnLocalPort():Int = synchronized(connectionsByLocalPort)
    {
        val unusedPort = (Int.MIN_VALUE..Int.MAX_VALUE).find {it !in connectionsByLocalPort.keys} ?: throw RuntimeException("failed to allocate port for new connection")
        connectionsByLocalPort[unusedPort] = SimpleConnection()
        unusedPort
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

        fun sendSilently(message:Message) = multiplexedConnectionAccess.withLock()
        {
            try
            {
                multiplexedOs.writeObject(message)
                multiplexedOs.flush()
            }
            catch (ex:Exception)
            {
                // ignore
            }
        }
    }

    private val reader = object:Thread()
    {
        val objI by lazy {multiplexedConnection.inputStream.let(::ObjectInputStream)}

        override tailrec fun run()
        {
            val message = try
            {
                objI.readObject() as Message
            }
            catch (ex:ClassCastException)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                close()
                return
            }
            synchronized(connectionsByLocalPort)
            {
                when (message)
                {
                    is Message.Connect -> inboundConnectsObjO.writeObject(message)
                    is Message.Accept -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                    is Message.Eof -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                    is Message.RequestEof -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                    is Message.Data -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                    is Message.Ack -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                    is Message.AckEof -> connectionsByLocalPort[message.dstPort]!!.receive(message)
                }.run {}
            }
            run()
        }

        init
        {
            start()
        }
    }

    /**
     * objects transmitted through the [multiplexedConnection].
     */
    private sealed class Message:Serializable
    {
        class Connect(val srcPort:Int):Message()
        class Accept(val srcPort:Int,val dstPort:Int):Message()

        /**
         * promise remote party that local party will no longer send any
         * [Message] objects to [dstPort] for this [Connection].
         */
        class Eof(val dstPort:Int):Message(),Serializable

        /**
         * request remote party to no longer send [Data] messages from [dstPort]
         * for this [Connection].
         */
        class RequestEof(val dstPort:Int):Message()
        class AckEof(val dstPort:Int):Message()
        class Data(val dstPort:Int,val payload:ByteArray):Message()
        class Ack(val dstPort:Int,val bytesRead:Int):Message()
    }

    private inner class SimpleConnection:Connection
    {
        val connectLatch = CountDownLatch(1)
        override val inputStream:InputStream get() = state.inputStream
        override val outputStream:OutputStream get() = state.outputStream
        override fun close() = state.close()
        var state:State = Connecting()
            private set
        fun receive(message:Message.Accept) = state.receive(message)
        fun receive(message:Message.Data) = state.receive(message)
        fun receive(message:Message.Ack) = state.receive(message)
        fun receive(message:Message.RequestEof) = state.receive(message)
        fun receive(message:Message.Eof) = state.receive(message)
        fun receive(message:Message.AckEof) = state.receive(message)

        inner abstract class State:Connection
        {
            abstract fun receive(message:Message.Accept)
            abstract fun receive(message:Message.Data)
            abstract fun receive(message:Message.Ack)
            abstract fun receive(message:Message.RequestEof)
            abstract fun receive(message:Message.Eof)
            abstract fun receive(message:Message.AckEof)
        }

        inner class Connecting:State()
        {
            override fun receive(message:Message.Accept)
            {
                state = Connected(message.dstPort,message.srcPort)
                connectLatch.countDown()
            }
            override fun receive(message:Message.Data) = throw UnsupportedOperationException()
            override fun receive(message:Message.Ack) = throw UnsupportedOperationException()
            override fun receive(message:Message.RequestEof) = throw UnsupportedOperationException()
            override fun receive(message:Message.Eof) = throw UnsupportedOperationException()
            override fun receive(message:Message.AckEof) = throw UnsupportedOperationException()
            override val inputStream:InputStream get() = throw UnsupportedOperationException()
            override val outputStream:OutputStream get() = throw UnsupportedOperationException()
            override fun close()
            {
                state = Disconnected()
                connectLatch.countDown()
            }
        }

        inner class Connected(val localPort:Int,val remotePort:Int):State()
        {
            private var oClosed = false
                set(value)
                {
                    field = value
                    if (iClosed && oClosed)
                    {
                        synchronized(connectionsByLocalPort)
                        {
                            if (connectionsByLocalPort[localPort] === this@SimpleConnection)
                            {
                                connectionsByLocalPort.remove(localPort)
                            }
                        }
                    }
                }
            private var iClosed = false
                set(value)
                {
                    field = value
                    if (iClosed && oClosed)
                    {
                        synchronized(connectionsByLocalPort)
                        {
                            if (connectionsByLocalPort[localPort] === this@SimpleConnection)
                            {
                                connectionsByLocalPort.remove(localPort)
                            }
                        }
                    }
                }
            private val inputStreamOs = SimplePipedOutputStream(RECV_WINDOW_SIZE_PER_CONNECTION)
            override val inputStream = object:AbstractInputStream()
            {
                private val pipeI = SimplePipedInputStream(inputStreamOs)
                override fun doRead(b:ByteArray,off:Int,len:Int):Int
                {
                    val bytesRead = pipeI.read(b,off,len)
                    synchronized(connectionsByLocalPort)
                    {
                        if (bytesRead > 0 && connectionsByLocalPort[localPort] === this@SimpleConnection)
                        {
                            sender.send(Message.Ack(remotePort,bytesRead))
                        }
                    }
                    return bytesRead
                }
                override fun doClose()
                {
                    synchronized(connectionsByLocalPort)
                    {
                        if (connectionsByLocalPort[localPort] === this@SimpleConnection)
                        {
                            try
                            {
                                sender.send(Message.RequestEof(remotePort))
                            }
                            catch (ex:Exception)
                            {
                                receive(Message.Eof(localPort))
                            }
                        }
                        doNothing()
                    }
                }
            }
            override val outputStream = RegulatedOutputStream(object:AbstractOutputStream()
            {
                override fun doClose()
                {
                    synchronized(connectionsByLocalPort)
                    {
                        setClosed()
                        try
                        {
                            sender.send(Message.Eof(remotePort))
                        }
                        catch (ex:Exception)
                        {
                            receive(Message.AckEof(localPort))
                        }
                    }
                }
                override fun doWrite(b:ByteArray,off:Int,len:Int)
                {
                    sender.send(Message.Data(remotePort,b.sliceArray(off..off+len-1)))
                }
            })
            init
            {
                outputStream.permit(RECV_WINDOW_SIZE_PER_CONNECTION)
            }

            override fun receive(message:Message.Accept) = throw UnsupportedOperationException()
            override fun receive(message:Message.Data) = inputStreamOs.write(message.payload)
            override fun receive(message:Message.Ack) = outputStream.permit(message.bytesRead)
            override fun receive(message:Message.RequestEof)
            {
                outputStream.close()
            }
            override fun receive(message:Message.Eof)
            {
                synchronized(connectionsByLocalPort)
                {
                    inputStreamOs.close()
                    sender.sendSilently(Message.AckEof(remotePort))
                    iClosed = true
                }
            }
            override fun receive(message:Message.AckEof)
            {
                oClosed = true
            }

            override fun close() = synchronized(connectionsByLocalPort)
            {
                inputStream.close()
                outputStream.close()
            }
        }

        inner class Disconnected:State()
        {
            override val inputStream:InputStream get() = throw UnsupportedOperationException()
            override val outputStream:OutputStream get() = throw UnsupportedOperationException()
            override fun close() = throw UnsupportedOperationException()
            override fun receive(message:Message.Accept) = throw UnsupportedOperationException()
            override fun receive(message:Message.Data) = throw UnsupportedOperationException()
            override fun receive(message:Message.Ack) = throw UnsupportedOperationException()
            override fun receive(message:Message.RequestEof) = throw UnsupportedOperationException()
            override fun receive(message:Message.Eof) = throw UnsupportedOperationException()
            override fun receive(message:Message.AckEof) = throw UnsupportedOperationException()
        }
    }
}
