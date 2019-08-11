package io.db4k.jdbc

import arrow.Kind
import arrow.core.Left
import arrow.core.Right
import arrow.fx.typeclasses.Async
import arrow.fx.typeclasses.ExitCase
import arrow.syntax.collections.prependTo
import arrow.typeclasses.MonadThrowSyntax
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

/**
 * JDBC operations syntax.  Example code:
 *
 * val queryRunner: JDBCOps<ForIO> = queryRunner(connection, IO.bracket())
 * IO.fx {
 *     val (rows) = queryRunner.query("<SQL QUERY>", ::rowMapper)
 *     // do something with rows here
 * }.fix().unsafeRunSync()
 *
 * fun rowMapper(rs: ResultSet): Kind<F, A> = fx.monadDefer {
 *     !effect {
 *         RowData(
 *             rs.getLong("id"),
 *             rs.getString("foo_column"),
 *             rs.getDate("bar_column"),
 *             ...
 *         )
 *     }
 * }
 *
 * @param F    The effect to run query programs within; must be at least as powerful as MonadDefer
 */

interface JDBCOps<F>: Async<F> {

    val connection: Connection

    fun rollback(): Kind<F, Unit> = fx.monadThrow { connection.rollback() }
    fun commit(): Kind<F, Unit> = fx.monadThrow { connection.commit() }

    fun <A> transaction(transactionUsage: suspend MonadThrowSyntax<F>.() -> A): Kind<F, A> =
            fx.monadThrow {
                connection.autoCommit = false
            }.bracketCase({ _, exitCase: ExitCase<Throwable> -> when(exitCase) {
                is ExitCase.Completed -> commit()
                is ExitCase.Canceled -> rollback()
                is ExitCase.Error -> rollback()
            } }, { fx.monadThrow { transactionUsage() } })


    private fun <A> statement(useStatement: suspend MonadThrowSyntax<F>.(Statement) -> A): Kind<F, A> =
            fx.monadThrow { connection.createStatement() }.bracket({ stmt -> fx.monadThrow { stmt.close() } }, { stmt -> fx.monadThrow { useStatement(stmt) } })

    private fun <A> executeQueryWithStatement(stmt: Statement, query: String, resultSetMapper: suspend MonadThrowSyntax<F>.(ResultSet) -> A): Kind<F, A> =
            fx.monadThrow { stmt.executeQuery(query) }.bracket({ resultSet -> fx.monadThrow { resultSet.close() } }, { resultSet -> fx.monadThrow { resultSetMapper(resultSet) } })

    private fun executeUpdateQueryWithStatement(stmt: Statement, query: String): Kind<F, Int> =
            fx.monadThrow { stmt.executeUpdate(query) }

    fun <A> query(query: String, resultSetMapper: suspend MonadThrowSyntax<F>.(ResultSet) -> A): Kind<F, A> =
            statement { stmt -> !executeQueryWithStatement(stmt, query, resultSetMapper) }
    
    fun execute(query: String): Kind<F, Boolean> =
            statement { stmt -> stmt.execute(query) }

    fun insert(query: String): Kind<F, Int> =
            statement { stmt -> !executeUpdateQueryWithStatement(stmt, query) }

    fun update(query: String): Kind<F, Int> = insert(query)

    fun <A> rowMapper(mapper: suspend MonadThrowSyntax<F>.(ResultSet) -> A): suspend MonadThrowSyntax<F>.(ResultSet) -> List<A> = { resultSet ->
        !tailRecM(emptyList<A>()) { list ->
            fx.monadThrow {
                if (resultSet.next()) {
                    val row = mapper(resultSet)
                    Left(row.prependTo(list))
                } else {
                    Right(list)
                }
            }
        }
    }


    /**
     * Execute a SQL query, using the given mapper to convert rows into objects of type A.  This method
     * manages the lifetime of the created Statement and ResultSet objects internally.  The mapper method
     * does not need to call ResultSet.close() and there is no need to call ResultSet.next(), as this method
     * ensures the ResultSet cursor is positioned at the correct row prior to calling the mapper.
     *
     * @param query   The SQL query to execute
     * @param mapper  The mapper that converts rows into objects of type A
     * @param A       The target type of the row conversion
     * @return        A list of objects of type A (one per row)
     */
    fun <A> rowMapperQuery(query: String, mapper: suspend MonadThrowSyntax<F>.(ResultSet) -> A): Kind<F, List<A>> =
            query(query, rowMapper(mapper))

    fun <A> doTransactionalRowMapperQuery(query: String, mapper: suspend MonadThrowSyntax<F>.(ResultSet) -> A): Kind<F, List<A>> =
            transaction { !rowMapperQuery(query, mapper) }

    companion object {
        /**
         * Create a query runner for a connection within the context of the given effect.
         *
         * @param connection    The database connection to use for queries.
         * @param BF            An instance of Bracket<F, Throwable> the query runner can use.
         * @return              A query runner that can execute queries.
         * @param <F>           The effect type to use.  Must be at least as powerful as MonadDefer.
         */
        fun <F> ops(connection: Connection, AS: Async<F>): JDBCOps<F> =
                object: JDBCOps<F>, Async<F> by AS {
                    override val connection = connection
                }
    }
}