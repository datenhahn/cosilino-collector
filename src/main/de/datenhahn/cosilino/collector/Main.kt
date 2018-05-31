package de.datenhahn.cosilino.collector

import com.beust.klaxon.JsonObject
import de.datenhahn.easymqtt.EasyMqtt
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils.createMissingTablesAndColumns
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import us.jimschubert.kopper.Parser
import java.io.StringReader
import java.math.BigDecimal
import java.sql.Connection

object DeviceData : Table() {
    val id = integer("id").primaryKey().autoIncrement()
    val deviceId = varchar("deviceId", 50).index()
    val roomHum = decimal("roomHum", 5, 2)
    val roomTemp = decimal("roomTemp", 5, 2)
    val heaterTemp = decimal("heaterTemp", 5, 2)
    val heaterPower = integer("heaterPower")
    val timestamp = datetime("timestamp").index()
}


fun parseHostPort(brokerurl: String): Pair<String, Int> {

    val parts = brokerurl.split(":")
    val host = parts[0]
    var port = 1883

    if (parts.size > 1) {
        port = parts[1].toInt()
    }

    return Pair(host, port)
}

fun main(args: Array<String>) {

    val parser = Parser()
    parser.setName("Cosilino Data Collector")
    parser.setApplicationDescription("Connects to the cosilino-gateway mqtt broker and persists the data to the database")

    parser.option("u", listOf("username"), description = "Mysql username", default = "cosilino")
    parser.option("p", listOf("password"), description = "Mysql password", default = "cosilino")
    parser.option("d", listOf("database"), description = "Mysql database", default = "cosilino")
    parser.option("b", listOf("brokerurl"), description = "Mqtt Broker", default = "cosilino-gateway:1883")

    val arguments = parser.parse(args)

    println("unparsedArgs=${arguments.joinToString()}")

    println(arguments.option("u"))

    val brokerUrl = arguments.option("b")

    val client: EasyMqtt

    try {
        if (brokerUrl != null) {
            val (mqttHost, mqttPort) = parseHostPort(brokerUrl)
            client = EasyMqtt(mqttHost, mqttPort)
        } else {
            client = EasyMqtt()
        }

        Database.connect("jdbc:mysql://127.0.0.1:3306/cosilino", user = "root", driver = "org.mariadb.jdbc.Driver")
        TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE

        transaction {
            createMissingTablesAndColumns(DeviceData)
        }
        client.subscribe("#", onMessage = { topic, message ->
            println(topic + " | " + message)
            try {
                var parsed = com.beust.klaxon.Parser().parse(StringReader(message)) as JsonObject
                transaction {
                    DeviceData.insert {
                        it[deviceId] = parsed.string("deviceId")!!
                        it[roomHum] = BigDecimal.valueOf(parsed.double("roomHum")!!)
                        it[roomTemp] = BigDecimal.valueOf(parsed.double("roomTemp")!!)
                        it[heaterTemp] = BigDecimal.valueOf(parsed.double("heaterTemp")!!)
                        it[heaterPower] = parsed.int("roomTemp")!!
                        it[timestamp] = DateTime.now()
                    }
                }

                println("done")
            } catch (e: Exception) {
                println("error during mqtt msg: ")
                e.printStackTrace()
            }

        }
        )

        while (true) {
            Thread.sleep(1000)
        }
    } catch (e: Exception) {
        println("Exception: " + e.message)
    }
}


