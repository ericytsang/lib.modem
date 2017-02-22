package com.github.ericytsang.lib.modem

object TestUtils
{
    fun getAllWorkerThreads():List<Thread>
    {
        val threadGroup = Thread.currentThread().threadGroup
        var threads = arrayOfNulls<Thread>(threadGroup.activeCount())
        while (threadGroup.enumerate(threads,true) === threads.size)
        {
            threads = arrayOfNulls<Thread>(threads.size*2)
        }
        return threads
            .filterNotNull()
            .filter {it != Thread.currentThread()}
            .filter {"Monitor Ctrl-Break" !in it.toString()}
            .filter {!it.isDaemon}
    }

    fun assertAllWorkerThreadsDead()
    {
        getAllWorkerThreads().forEach {
            val stacktrace = it.stackTrace.toList()
            check(stacktrace.isEmpty())
            {
                throw Exception("thread not dead: ${stacktrace.joinToString("\n    ")}")
            }
        }
    }
}
