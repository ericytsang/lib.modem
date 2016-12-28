package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.net.connection.Connection
import com.github.ericytsang.lib.net.host.TcpClient
import com.github.ericytsang.lib.net.host.TcpServer
import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

/**
 * Created by surpl on 11/20/2016.
 */
class ModemTest
{
    companion object
    {
        const val TEST_PORT = 55652
    }
    val conn1:Connection
    val conn2:Connection
    init
    {
        val tcpClient = TcpClient.anySrcPort()
        val tcpServer = TcpServer(TEST_PORT)
        val q = LinkedBlockingQueue<Connection>()
        kotlin.concurrent.thread()
        {
            q.put(tcpServer.accept())
        }
        conn1 = tcpClient.connect(TcpClient.Address(InetAddress.getByName("localhost"),TEST_PORT))
        conn2 = q.take()
        tcpServer.close()
    }

    @org.junit.After
    fun teardown()
    {
        println("fun teardown() ===============================")
        Thread.sleep(500)
        conn1.close()
        conn2.close()
        Thread.sleep(500)
    }

    @org.junit.Test
    fun instantiateTest()
    {
        Modem(conn1)
        Modem(conn2)
    }

    @org.junit.Test
    fun connectAcceptTest()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val t1 = thread {m1.connect(Unit)}
        Thread.sleep(10)
        assert(t1.isAlive)
        val t2 = thread {m2.accept()}
        Thread.sleep(10)
        assert(!t1.isAlive)
        assert(!t2.isAlive)
        t1.join()
        t2.join()
        m1.close()
        m2.close()
    }

    @org.junit.Test
    fun concurrentConversationsTest1()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val q = LinkedBlockingQueue<Connection>()
        thread {(1..5).forEach {q.put(m2.accept())}}
        val conn1s = (1..5).map {m1.connect(Unit)}
        val conn2s = (1..5).map {q.take()}
        // have 2 connections talk concurrently
        val threads = listOf(
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);conn1s[0].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);conn1s[1].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);assert(conn2s[0].inputStream.let(::DataInputStream).readInt() == it)}},
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);assert(conn2s[1].inputStream.let(::DataInputStream).readInt() == it)}})
        threads.forEach {it.join()}
        m1.close()
        m2.close()
    }

    @Test
    fun concurrentConversationsTest2()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val q = LinkedBlockingQueue<Connection>()
        thread {(1..5).forEach {q.put(m2.accept())}}
        val conn1s = (1..5).map {m1.connect(Unit)}
        val conn2s = (1..5).map {q.take()}
        // have 1 connection send a lot, but corresponding one not receive
        val hangingThreads = listOf(
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);conn1s[0].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Short.MIN_VALUE..0).forEach {if (it%1000 == 0) println(it);assert(conn2s[0].inputStream.let(::DataInputStream).readInt() == it)}})
        // have another connection send and receive as normal while the first one is blocked
        val joinableThreads = listOf(
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);conn1s[1].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);assert(conn2s[1].inputStream.let(::DataInputStream).readInt() == it)}})
        joinableThreads.forEach {it.join()}
        Thread.sleep(250)
        hangingThreads.all {it.isAlive}
        m1.close()
        m2.close()
    }

    @Test
    fun closingBreaksOngoingConnect()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        thread {
            Thread.sleep(100)
            m1.close()
        }
        try
        {
            m1.connect(Unit)
            assert(false)
        }
        catch (ex:Exception)
        {
            ex.printStackTrace()
        }
        m1.close()
        m2.close()
    }

    @Test
    fun closingBreaksOngoingAccept()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        thread {
            Thread.sleep(100)
            m1.close()
        }
        try
        {
            m1.accept()
            assert(false)
        }
        catch (ex:Exception)
        {
            ex.printStackTrace()
        }
        m1.close()
        m2.close()
    }

    @Test
    fun halfCloseTest()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val q = LinkedBlockingQueue<Connection>()
        thread {(1..1).forEach {q.put(m2.accept())}}
        val conn1 = m1.connect(Unit)
        val conn2 = q.take()
        // have 2 connections talk concurrently
        conn1.outputStream.close()
        val threads = listOf(
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);conn2.outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Short.MIN_VALUE..Short.MAX_VALUE).forEach {if (it%1000 == 0) println(it);assert(conn1.inputStream.let(::DataInputStream).readInt() == it)}})
        threads.forEach {it.join()}
        conn2.outputStream.close()
        m1.close()
        m2.close()
    }
}
