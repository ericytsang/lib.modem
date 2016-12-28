package com.github.ericytsang.lib.modem

import org.junit.Test
import kotlin.concurrent.thread

/**
 * Created by surpl on 10/28/2016.
 */
class MailboxTest
{
    val mailbox = Mailbox<Int,String>()

    @Test
    fun generalUseCaseTest()
    {
        val mail = "hello!"
        val address = 5
        val recipient = mailbox.expectMail(address)
        println("expecting message")
        thread {
            Thread.sleep(100)
            mailbox.putMail(address,mail)
            println("message sent!")
        }
        println("awaiting message")
        val message = recipient.awaitMail()
        println("we got a message")
        assert(message == mail)
    }

    @Test
    fun throwOnCloseTestConcurrent()
    {
        val address = 5
        val recipient = mailbox.expectMail(address)
        println("expecting message")
        val t = thread {
            recipient.awaitMail()
        }
        println("awaiting message")
        Thread.sleep(100)
        assert(t.isAlive)
        mailbox.close()
        println("mailbox closed")
        Thread.sleep(100)
        assert(!t.isAlive)
    }

    @Test
    fun throwOnCloseTest()
    {
        mailbox.close()
        try
        {
            mailbox.expectMail(0).awaitMail()
            assert(false) {"no exception thrown"}
        }
        catch (ex:IllegalStateException)
        {
            // it threw the exception we wanted
        }
        catch (ex:Exception)
        {
            assert(false) {"exception of wrong type thrown"}
        }
    }
}
