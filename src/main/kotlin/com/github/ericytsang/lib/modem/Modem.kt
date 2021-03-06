package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.regulatedstream.RegulatedOutputStream
import com.github.ericytsang.lib.abstractstream.AbstractInputStream
import com.github.ericytsang.lib.abstractstream.AbstractOutputStream
import com.github.ericytsang.lib.net.connection.Connection
import com.github.ericytsang.lib.net.host.Client
import com.github.ericytsang.lib.net.host.Server
import com.github.ericytsang.lib.onlysetonce.OnlySetOnce
import com.github.ericytsang.lib.simplepipestream.SimplePipedInputStream
import com.github.ericytsang.lib.simplepipestream.SimplePipedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Modem(val multiplexedConnection:Connection,backlogSize:Int = Int.MAX_VALUE):Client<Unit>,Server
{
    companion object
    {
        const val RECV_WINDOW_SIZE_PER_CONNECTION = 8*1024
    }

    private val createStackTrace = Thread.currentThread().stackTrace

    override fun connect(remoteAddress:Unit):Connection
    {
        val connection = connectionsByLocalPortMutex.withLock()
        {
            val localPort = makeNewConnectionOnLocalPort()
            val connection = connectionsByLocalPort[localPort]!!
            sender.send(Message.Connect(localPort))
            connection
        }
        connection.connectLatch.await()
        val connectionState = connection.state
            as? SimpleConnection.Connected
            ?: throwClosedExceptionIfClosedOrRethrow(ConnectException("connection refused"))
        sender.send(Message.Ack(connectionState.remotePort,RECV_WINDOW_SIZE_PER_CONNECTION))
        return connection
    }

    override fun accept():Connection
    {
        val inboundConnect = inboundConnectQueueAccess.withLock()
        {
            while (inboundConnectQueue.isEmpty() && closeStacktrace == null)
            {
                inboundConnectQueuePutOrCloseEvent.await()
            }
            if (closeStacktrace == null)
            {
                inboundConnectQueue.take()
            }
            else
            {
                throwClosedExceptionIfClosedOrRethrow(RuntimeException())
            }
        }
        connectionsByLocalPortMutex.withLock()
        {
            val localPort = makeNewConnectionOnLocalPort()
            val connection = connectionsByLocalPort[localPort]!!
            connection.receive(Message.Accept(inboundConnect.srcPort,localPort))
            sender.send(Message.Accept(localPort,inboundConnect.srcPort))
            sender.send(Message.Ack(inboundConnect.srcPort,RECV_WINDOW_SIZE_PER_CONNECTION))
            return connection
        }
    }

    private var closeStacktrace:Array<StackTraceElement>? by OnlySetOnce()

    override fun close()
    {
        try
        {
            closeStacktrace = Thread.currentThread().stackTrace
            multiplexedConnection.close()
            reader.join(10000)
            if (reader.isAlive || sender.isAlive)
            {
                throw RuntimeException("reader and/or sender thread not dying..." +
                    "reader thread stacktrace:${reader.stackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}" +
                    "sender thread stacktrace:${sender.stackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}")
            }
        }
        catch (ex:OnlySetOnce.Exception)
        {
            // ignore exception...we only want to do close stuff once...
        }
    }

    private val inboundConnectQueue = LinkedBlockingQueue<Message.Connect>(backlogSize)
    private val inboundConnectQueueAccess = ReentrantLock()
    private val inboundConnectQueuePutOrCloseEvent = inboundConnectQueueAccess.newCondition()

    private val connectionsByLocalPortMutex = ReentrantLock()
    private val connectionsByLocalPort = linkedMapOf<Int,SimpleConnection>()

    private fun makeNewConnectionOnLocalPort():Int = connectionsByLocalPortMutex.withLock()
    {
        val unusedPort = (Int.MIN_VALUE..Int.MAX_VALUE).find {it !in connectionsByLocalPort.keys} ?: throw RuntimeException("failed to allocate port for new connection")
        connectionsByLocalPort[unusedPort] = SimpleConnection()
        unusedPort
    }

    private val sender = object:Thread()
    {
        /**
         * monitor that must be held before writing to [multiplexedOs].
         */
        private val messages = LinkedBlockingQueue<()->Message>()

        private val multiplexedOs = multiplexedConnection.outputStream.buffered()

        fun send(message:Message)
        {
            if (closeStacktrace != null)
            {
                throwClosedExceptionIfClosedOrRethrow(IllegalStateException("already closed"))
            }
            messages.put({message})
        }

        fun sendSilently(message:Message)
        {
            try
            {
                send(message)
            }
            catch (ex:Exception)
            {
                // ignore
            }
        }

        override tailrec fun run()
        {
            val message = try
            {
                messages.take().invoke()
            }
            catch (ex:InterruptedException)
            {
                return
            }
            try
            {
                message.serialize(multiplexedOs)
                if (messages.isEmpty()) multiplexedOs.flush()
            }
            catch (ex:Exception)
            {
                multiplexedConnection.close()
                RuntimeException("underlying stream closed for modem created at:${createStackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}",ex).printStackTrace(System.out)
                return
            }
            run()
        }

        override fun interrupt()
        {
            messages.put {throw InterruptedException()}
            super.interrupt()
        }

        init
        {
            start()
        }
    }

    private val reader = object:Thread()
    {
        val dataI by lazy {multiplexedConnection.inputStream.buffered()}

        override tailrec fun run()
        {
            val message = try
            {
                Message.parse(dataI)
            }
            catch (ex:ArrayIndexOutOfBoundsException)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                try
                {
                    closeStacktrace = Thread.currentThread().stackTrace
                }
                catch (ex:OnlySetOnce.Exception)
                {
                    // ignore
                }

                RuntimeException("underlying stream closed for modem created at:${createStackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}",ex).printStackTrace(System.out)

                // abort pending accepts
                inboundConnectQueueAccess.withLock()
                {
                    inboundConnectQueuePutOrCloseEvent.signalAll()
                }

                // close the sender
                sender.interrupt()
                sender.join()

                // close all existing de-multiplexed streams.
                connectionsByLocalPortMutex.withLock()
                {
                    connectionsByLocalPort.values.toList()
                        .forEach(SimpleConnection::modemDeadClose)
                }
                return
            }
            connectionsByLocalPortMutex.withLock()
            {
                when (message)
                {
                    is Message.Connect ->
                    {
                        if (inboundConnectQueue.offer(message))
                        {
                            inboundConnectQueueAccess.withLock()
                            {
                                inboundConnectQueuePutOrCloseEvent.signal()
                            }
                        }
                        else
                        {
                            sender.send(Message.RejectConnect(message.srcPort))
                        }
                    }
                    is Message.RejectConnect -> connectionsByLocalPort[message.dstPort]!!.receive(message)
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

    private fun throwClosedExceptionIfClosedOrRethrow(cause:Throwable):Nothing
    {
        if (closeStacktrace != null)
        {
            throw IllegalStateException("modem created at:${createStackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}has been closed at the following stacktrace:\n${closeStacktrace!!.joinToString("\n","\nvvvv\n","\n^^^^\n")}",cause)
        }
        else throw cause
    }

    private inner class SimpleConnection:Connection
    {
        val connectLatch = CountDownLatch(1)
        override val inputStream:InputStream get() = state.inputStream
        override val outputStream:OutputStream get() = state.outputStream
        override fun close() = state.close()
        var state:State = Connecting()
            private set
        fun receive(message:Message.RejectConnect) = state.receive(message)
        fun receive(message:Message.Accept) = state.receive(message)
        fun receive(message:Message.Data) = state.receive(message)
        fun receive(message:Message.Ack) = state.receive(message)
        fun receive(message:Message.RequestEof) = state.receive(message)
        fun receive(message:Message.Eof) = state.receive(message)
        fun receive(message:Message.AckEof) = state.receive(message)
        fun modemDeadClose() = state.modemDeadClose()

        inner abstract class State:Connection
        {
            abstract fun receive(message:Message.RejectConnect)
            abstract fun receive(message:Message.Accept)
            abstract fun receive(message:Message.Data)
            abstract fun receive(message:Message.Ack)
            abstract fun receive(message:Message.RequestEof)
            abstract fun receive(message:Message.Eof)
            abstract fun receive(message:Message.AckEof)
            abstract fun modemDeadClose()
        }

        inner class Connecting:State()
        {
            override fun receive(message:Message.Accept)
            {
                state = Connected(message.dstPort,message.srcPort)
                connectLatch.countDown()
            }

            override fun receive(message:Message.RejectConnect)
            {
                if (connectionsByLocalPort[message.dstPort] === this@SimpleConnection)
                {
                    connectionsByLocalPort.remove(message.dstPort)
                }
                state = Disconnected()
                connectLatch.countDown()
            }
            override fun receive(message:Message.Data) = throw UnsupportedOperationException()
            override fun receive(message:Message.Ack) = throw UnsupportedOperationException()
            override fun receive(message:Message.RequestEof) = throw UnsupportedOperationException()
            override fun receive(message:Message.Eof) = throw UnsupportedOperationException()
            override fun receive(message:Message.AckEof) = throw UnsupportedOperationException()
            override val inputStream:InputStream get() = throw UnsupportedOperationException()
            override val outputStream:OutputStream get() = throw UnsupportedOperationException()
            override fun close() = throw UnsupportedOperationException()
            override fun modemDeadClose()
            {
                state = Disconnected()
                connectLatch.countDown()
            }
        }

        inner class Connected(val localPort:Int,val remotePort:Int):State()
        {
            private val completeShutdownLatch = CountDownLatch(1)
            private fun removeIfIsShutdownCompletely()
            {
                if (iClosed && oClosed)
                {
                    connectionsByLocalPortMutex.withLock()
                    {
                        if (connectionsByLocalPort[localPort] === this@SimpleConnection)
                        {
                            connectionsByLocalPort.remove(localPort)
                        }
                    }
                    completeShutdownLatch.countDown()
                }
            }
            private var oClosed = false
                set(value)
                {
                    field = value
                    removeIfIsShutdownCompletely()
                }
            private var iClosed = false
                set(value)
                {
                    field = value
                    removeIfIsShutdownCompletely()
                }
            private val inputStreamOs = SimplePipedOutputStream(RECV_WINDOW_SIZE_PER_CONNECTION)
            override val inputStream = object:AbstractInputStream()
            {
                private val pipeI = SimplePipedInputStream(inputStreamOs)
                override fun doRead(b:ByteArray,off:Int,len:Int):Int
                {
                    val bytesRead = pipeI.read(b,off,len)
                    connectionsByLocalPortMutex.withLock()
                    {
                        if (!iClosed && bytesRead > 0)
                        {
                            sender.sendSilently(Message.Ack(remotePort,bytesRead))
                        }
                    }
                    return bytesRead
                }
                override fun oneShotClose()
                {
                    connectionsByLocalPortMutex.withLock()
                    {
                        if (!iClosed)
                        {
                            sender.sendSilently(Message.RequestEof(remotePort))
                        }
                    }
                }
            }
            override val outputStream = RegulatedOutputStream(object:AbstractOutputStream()
            {
                private val mutex = ReentrantLock()
                override fun oneShotClose() = mutex.withLock()
                {
                    sender.sendSilently(Message.Eof(remotePort))
                }
                override fun doWrite(b:ByteArray,off:Int,len:Int) = mutex.withLock()
                {
                    check(!isClosed)
                    sender.send(Message.Data(remotePort,b.sliceArray(off..off+len-1)))
                }
            })

            override fun receive(message:Message.RejectConnect) = throw UnsupportedOperationException()
            override fun receive(message:Message.Accept) = throw UnsupportedOperationException()
            override fun receive(message:Message.Data) = inputStreamOs.write(message.payload)
            override fun receive(message:Message.Ack) = outputStream.permit(message.bytesRead)
            override fun receive(message:Message.RequestEof)
            {
                outputStream.close()
            }
            override fun receive(message:Message.Eof)
            {
                inputStreamOs.close()
                sender.sendSilently(Message.AckEof(remotePort))
                iClosed = true
            }
            override fun receive(message:Message.AckEof)
            {
                oClosed = true
            }

            override fun close()
            {
                inputStream.close()
                outputStream.close()
                awaitShutdown()
            }

            override fun modemDeadClose()
            {
                receive(Message.Eof(0))
                receive(Message.AckEof(0))
                awaitShutdown()
            }

            private fun awaitShutdown()
            {
                if (!completeShutdownLatch.await(120,TimeUnit.SECONDS))
                {
                    throw TimeoutException("" +
                        "failed to close de-multiplexed stream....reader " +
                        "reader stacktrace:${reader.stackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}" +
                        "sender stacktrace:${sender.stackTrace.joinToString("\n","\nvvvv\n","\n^^^^\n")}" +
                        "oClosed: $oClosed; iClosed: $iClosed")
                }
            }
        }

        inner class Disconnected:State()
        {
            override val inputStream:InputStream get() = throw UnsupportedOperationException()
            override val outputStream:OutputStream get() = throw UnsupportedOperationException()
            override fun close() = Unit
            override fun receive(message:Message.RejectConnect) = Unit
            override fun receive(message:Message.Accept) = Unit
            override fun receive(message:Message.Data) = Unit
            override fun receive(message:Message.Ack) = Unit
            override fun receive(message:Message.RequestEof) = Unit
            override fun receive(message:Message.Eof) = Unit
            override fun receive(message:Message.AckEof) = Unit
            override fun modemDeadClose() = Unit
        }
    }
}
