package io.db4k.jdbc

import arrow.core.extensions.list.foldable.foldLeft
import arrow.fx.IO
import arrow.fx.extensions.io.async.async
import arrow.fx.fix
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Copyright (C) 2019 Medtronic PLC.  All Rights Reserved.
 */

class H2IntegrationTests {

    data class TestRecord(val id: Int, val name: String)
    data class TestSummary(val sum: Int, val cc: String)

    @Test
    fun testCommit() {
        val connMgr = JDBCConnectionManager.connectionManager(
            "jdbc:h2:mem:db4k_h2_integration", "sa", "", IO.async()
        )
        connMgr.connection { connection ->
            val jdbcOps = JDBCOps.ops(connection, IO.async())
            with(jdbcOps) {
                !execute("CREATE TABLE records (id INTEGER, name VARCHAR(255))")
                !execute("CREATE TABLE summary (sum INTEGER, cc VARCHAR(255))")
                !transaction {
                    !insert("INSERT INTO records (id, name) VALUES (1, 'foo')")
                    !insert("INSERT INTO records (id, name) VALUES (2, 'bar')")
                }
                !transaction {
                    val (records) = query("SELECT id, name FROM records ORDER BY id ASC",
                        rowMapper { rs -> with(rs) { TestRecord(int("id"), string("name")) } })
                    val summary = records.foldLeft(TestSummary(0, ""),
                            { s, r -> TestSummary(s.sum + r.id, "${s.cc}:${r.name}") })
                    !insert("INSERT INTO summary (sum, cc) VALUES (${summary.sum}, '${summary.cc}')")
                }
                val (summaryRecord) = transaction {
                    !query("SELECT sum, cc FROM summary",
                            rowMapper { rs -> with(rs) { TestSummary(int("sum"), string("cc")) } })
                }
                assertTrue(
                        summaryRecord.size == 1 && summaryRecord[0].sum == 3 && summaryRecord[0].cc == ":bar:foo",
                        "Assertion failed: ${summaryRecord.size} ${summaryRecord[0].sum} ${summaryRecord[0].cc}"
                )
            }
        }.fix().unsafeRunSync()
    }
}