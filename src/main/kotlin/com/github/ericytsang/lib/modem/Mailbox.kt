package com.github.ericytsang.lib.modem

import java.io.Closeable
import java.util.LinkedHashMap
import java.util.NoSuchElementException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Created by surpl on 10/24/2016.
 */
class Mailbox<Address:Any,Mail>:Closeable
{
    var isClosed = false
        private set

    private val mutex = ReentrantLock()

    private val recipients:MutableMap<Address,SimpleRecipient> = LinkedHashMap()

    fun putMail(address:Address,mail:Mail) = mutex.withLock()
    {
        if (isClosed) throw IllegalStateException("closed")
        val recipient = recipients.remove(address)
        if (recipient != null)
        {
            recipient.receivedMail.put({mail})
        }
        else
        {
            throw NoSuchElementException("no recipient found for address")
        }
    }

    /**
     * waits for main for the specified address. asserts that it is the only
     * recipient waiting for mail at that address. returns the mail once
     * received.
     */
    fun expectMail(address:Address):Recipient<Address,Mail> = mutex.withLock()
    {
        if (isClosed) throw IllegalStateException("closed")
        if (recipients[address] != null)
        {
            throw NoSuchElementException("address already awaited")
        }
        else
        {
            val recipient = SimpleRecipient(address)
            recipients[address] = recipient
            recipient
        }
    }

    fun isAwaited(address:Address):Boolean
    {
        return address in recipients.keys
    }

    override fun close() = mutex.withLock()
    {
        isClosed = true
        recipients.values.forEach {it.receivedMail.put({throw IllegalStateException("closed")})}
    }

    interface Recipient<out Address,out Mail>
    {
        val address:Address
        fun awaitMail():Mail
    }

    private inner class SimpleRecipient(override val address:Address):Recipient<Address,Mail>
    {
        private var isClosed = false
        private var waitingThread:Thread? = null
        val receivedMail:BlockingQueue<()->Mail> = ArrayBlockingQueue(1)
        override fun awaitMail():Mail
        {
            waitingThread = Thread.currentThread()
            if (isClosed)
            {
                throw IllegalStateException("mailbox closed")
            }
            try
            {
                return receivedMail.take().invoke()
            }
            catch (ex:Exception)
            {
                if (isClosed)
                {
                    return awaitMail()
                }
                else
                {
                    throw ex
                }
            }
        }
    }
}
