package io.db4k.jdbc

import arrow.Kind
import arrow.fx.typeclasses.Async
import arrow.typeclasses.MonadThrowSyntax
import java.sql.Connection
import java.sql.DriverManager

/**
 * Copyright (C) 2019 Medtronic PLC.  All Rights Reserved.
 */
interface JDBCConnectionManager<F>: Async<F> {
    val dbUrl: String
    val username: String
    val password: String

    fun connection(usage: suspend MonadThrowSyntax<F>.(Connection) -> Unit): Kind<F, Unit> =
            fx.monadThrow { DriverManager.getConnection(dbUrl, username, password) }.bracket(
                    { conn -> fx.monadThrow { conn.close() } },
                    { conn -> fx.monadThrow { usage(conn) } }
            )

    companion object {
        fun <F> connectionManager(
                dbUrl: String,
                username: String,
                password: String,
                AS: Async<F>
        ): JDBCConnectionManager<F> =
                object: JDBCConnectionManager<F>, Async<F> by AS {
                    override val dbUrl = dbUrl
                    override val username = username
                    override val password = password
                }
    }
}