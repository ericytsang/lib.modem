package com.github.ericytsang.lib.modem

import kotlin.system.measureTimeMillis

object TestUtils
{
    fun getAllWorkerThreads(excludeCurrentThread:Boolean,exceptions:Set<String>):List<Thread>
    {
        val threadGroup = Thread.currentThread().threadGroup
        var threads = arrayOfNulls<Thread>(threadGroup.activeCount())
        while (threadGroup.enumerate(threads,true) === threads.size)
        {
            threads = arrayOfNulls<Thread>(threads.size*2)
        }
        return threads
            .filterNotNull()
            .filter {!excludeCurrentThread || it != Thread.currentThread()}
            .filter {it.name !in exceptions}
            .filter {!it.isDaemon}
    }

    fun assertAllWorkerThreadsDead(exceptions:Set<String> = emptySet(),timeout:Long = 1)
    {
        // get worker threads
        val workerThreads = getAllWorkerThreads(true,exceptions)

        // wait a maximum of timeout ms for threads to die
        if (timeout > 0)
        {
            @Suppress("NAME_SHADOWING")
            var timeout = timeout
            for (thread in workerThreads)
            {
                timeout -= measureTimeMillis {
                    thread.join(timeout)
                }
                if (timeout <= 0) break
            }
        }

        // wait indefinitely for threads to die
        else
        {
            for (thread in workerThreads)
            {
                thread.join()
            }
        }

        // throw exceptions for threads that are still alive
        val stackTraces = workerThreads
            .filter {it.isAlive}
            .map {it.stackTrace.joinToString(prefix = "\n${it.name}:\n",separator = "\n    ")}
        if (stackTraces.isNotEmpty())
        {
            throw Exception("threads not dead: \n${stackTraces.joinToString(prefix = "\n\nvvvvvvvv\n\n",separator = "\n\n", postfix = "\n\n^^^^^^^^\n\n")}")
        }
    }
}
