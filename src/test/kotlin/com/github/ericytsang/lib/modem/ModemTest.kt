package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.concurrent.future
import com.github.ericytsang.lib.net.connection.Connection
import com.github.ericytsang.lib.net.host.TcpClient
import com.github.ericytsang.lib.net.host.TcpServer
import org.junit.After
import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

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

    @After
    fun teardown()
    {
        println("fun teardown() ===============================")
        Thread.sleep(100)
        conn1.close()
        conn2.close()
        TestUtils.assertAllWorkerThreadsDead(emptySet(),100)
        Thread.sleep(100)
    }

    @Test
    fun instantiateTest()
    {
        Modem(conn1)
        Modem(conn2)
    }

    @Test
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

    @Test
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
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);conn1s[0].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);conn1s[1].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);assert(conn2s[0].inputStream.let(::DataInputStream).readInt() == it)}},
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);assert(conn2s[1].inputStream.let(::DataInputStream).readInt() == it)}})
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
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);conn1s[0].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Byte.MIN_VALUE..0).forEach {if (it%10 == 0) println(it);assert(conn2s[0].inputStream.let(::DataInputStream).readInt() == it)}})
        // have another connection send and receive as normal while the first one is blocked
        val joinableThreads = listOf(
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);conn1s[1].outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);assert(conn2s[1].inputStream.let(::DataInputStream).readInt() == it)}})
        joinableThreads.forEach {it.join()}
        Thread.sleep(250)
        hangingThreads.all {it.isAlive}
        m1.close()
        m2.close()
        hangingThreads.forEach {it.stop()}
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
            throw AssertionError()
        }
        catch (ex:AssertionError)
        {
            throw ex
        }
        catch (ex:Exception)
        {
            ex.printStackTrace(System.out)
        }
        m1.close()
        m2.close()
    }

    @Test
    fun closingBreaksOngoingConnections()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val q = LinkedBlockingQueue<Connection>()
        thread {(1..5).forEach {q.put(m2.accept())}}
        (1..5).map {m1.connect(Unit)}
        val conn2s = (1..5).map {q.take()}
        // have 2 connections talk concurrently
        val threads = conn2s.map {thread {it.inputStream.read()}}
        m1.close()
        threads.forEach {it.join()}
        m2.close()
    }

    @Test
    fun overflowConnectsGetRefused()
    {
        val m1 = Modem(conn1,3)
        val m2 = Modem(conn2)
        val threads = (1..3).map {thread {m2.connect(Unit)}}
        Thread.sleep(100)
        check(threads.all {it.isAlive})
        try
        {
            m2.connect(Unit)
            throw AssertionError()
        }
        catch (ex:AssertionError)
        {
            throw ex
        }
        catch (ex:Exception)
        {
            ex.printStackTrace(System.out)
        }
        m1.close()
        threads.forEach {it.join()}
        m2.close()
    }

    @Test
    fun closeUnderlyingConnectionAbortsConnect1()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val thread = thread {
            try
            {
                m2.connect(Unit)
                throw AssertionError()
            }
            catch (ex:AssertionError)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                ex.printStackTrace(System.out)
            }
        }
        Thread.sleep(100)
        check(thread.isAlive)
        conn1.close()
        thread.join()
        m2.close()
    }

    @Test
    fun closeUnderlyingConnectionAbortsConnect2()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val thread = thread {
            try
            {
                m2.connect(Unit)
                throw AssertionError()
            }
            catch (ex:AssertionError)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                ex.printStackTrace(System.out)
            }
        }
        Thread.sleep(100)
        check(thread.isAlive)
        conn2.close()
        thread.join()
        m2.close()
    }

    @Test
    fun closeUnderlyingConnectionAbortsAccept1()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val thread = thread {
            try
            {
                m2.accept()
                throw AssertionError()
            }
            catch (ex:AssertionError)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                ex.printStackTrace(System.out)
            }
        }
        Thread.sleep(100)
        check(thread.isAlive)
        conn1.close()
        thread.join()
        m2.close()
    }

    @Test
    fun closeUnderlyingConnectionAbortsAccept2()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val thread = thread {
            try
            {
                m2.accept()
                throw AssertionError()
            }
            catch (ex:AssertionError)
            {
                throw ex
            }
            catch (ex:Exception)
            {
                ex.printStackTrace(System.out)
            }
        }
        Thread.sleep(100)
        check(thread.isAlive)
        conn2.close()
        thread.join()
        m2.close()
    }

    @Test
    fun closingBreaksOngoingRead()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val t = future {
            val connection = m2.accept()
            check(connection.inputStream.read() == -1)
        }
        m1.connect(Unit)
        Thread.sleep(100)
        m1.close()
        t.get()
        m2.close()
    }

    @Test
    fun closingUnderlyingConnectionBreaksOngoingRead1()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val t = future {
            val connection = m2.accept()
            check(connection.inputStream.read() == -1)
        }
        m1.connect(Unit)
        Thread.sleep(100)
        conn1.close()
        t.get()
        m2.close()
    }

    @Test
    fun closingUnderlyingConnectionBreaksOngoingRead2()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val t = future {
            val connection = m2.accept()
            check(connection.inputStream.read() == -1)
        }
        m1.connect(Unit)
        Thread.sleep(100)
        conn2.close()
        t.get()
        m2.close()
    }

    @Test
    fun closingBreaksOngoingAccept()
    {
        val m1 = Modem(conn1)
        val m2 = Modem(conn2)
        val t = thread {
            Thread.sleep(100)
            m1.close()
        }
        try
        {
            m1.accept()
            throw AssertionError()
        }
        catch (ex:AssertionError)
        {
            throw ex
        }
        catch (ex:Exception)
        {
            ex.printStackTrace(System.out)
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
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);conn2.outputStream.let(::DataOutputStream).writeInt(it)}},
            thread {(Byte.MIN_VALUE..Byte.MAX_VALUE).forEach {if (it%10 == 0) println(it);assert(conn1.inputStream.let(::DataInputStream).readInt() == it)}})
        threads.forEach {it.join()}
        conn2.outputStream.close()
        Thread.sleep(100)
        m1.close()
        m2.close()
    }
}
