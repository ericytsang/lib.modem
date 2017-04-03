package com.github.ericytsang.lib.modem

import com.github.ericytsang.lib.net.connection.Connection
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream

internal sealed class Message
{
    companion object
    {
        enum class Type
        {REJECT_CONNECT, CONNECT, ACCEPT, EOF, REQUEST_EOF, ACK_EOF, DATA, ACK }

        private val typeValues = Type.values()
        fun parse(inputStream:InputStream):Message
        {
            val i = DataInputStream(inputStream)
            val readResult = i.read()
            if (readResult == -1) throw EOFException()
            val type = typeValues[readResult]
            return when (type)
            {
                Type.REJECT_CONNECT -> RejectConnect(i.readInt())
                Type.CONNECT -> Connect(i.readInt())
                Type.ACCEPT ->
                {
                    val sPort = i.readInt()
                    val dPort = i.readInt()
                    Accept(sPort,dPort)
                }
                Type.EOF -> Eof(i.readInt())
                Type.REQUEST_EOF -> RequestEof(i.readInt())
                Type.ACK_EOF -> AckEof(i.readInt())
                Type.DATA ->
                {
                    val port = i.readInt()
                    val payload = ByteArray(i.readInt())
                    i.readFully(payload)
                    Data(port,payload)
                }
                Type.ACK ->
                {
                    val port = i.readInt()
                    val nRead = i.readInt()
                    Ack(port,nRead)
                }
            }
        }
    }

    class RejectConnect(val dstPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.REJECT_CONNECT.ordinal)
            o.writeInt(dstPort)
        }
    }

    class Connect(val srcPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.CONNECT.ordinal)
            o.writeInt(srcPort)
        }
    }

    class Accept(val srcPort:Int,val dstPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.ACCEPT.ordinal)
            o.writeInt(srcPort)
            o.writeInt(dstPort)
        }
    }

    /**
     * promise remote party that local party will no longer send any
     * [Message] objects to [dstPort] for this [Connection].
     */
    class Eof(val dstPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.EOF.ordinal)
            o.writeInt(dstPort)
        }
    }

    /**
     * request remote party to no longer send [Data] messages from [dstPort]
     * for this [Connection].
     */
    class RequestEof(val dstPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.REQUEST_EOF.ordinal)
            o.writeInt(dstPort)
        }
    }

    class AckEof(val dstPort:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.ACK_EOF.ordinal)
            o.writeInt(dstPort)
        }
    }

    class Data(val dstPort:Int,val payload:ByteArray):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.DATA.ordinal)
            o.writeInt(dstPort)
            o.writeInt(payload.size)
            o.write(payload)
        }
    }

    class Ack(val dstPort:Int,val bytesRead:Int):Message()
    {
        override fun serialize(outputStream:OutputStream)
        {
            val o = DataOutputStream(outputStream)
            o.write(Type.ACK.ordinal)
            o.writeInt(dstPort)
            o.writeInt(bytesRead)
        }
    }

    abstract fun serialize(outputStream:OutputStream)
}
