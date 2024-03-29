package io.db4k.jdbc

import java.sql.ResultSet
import java.util.*

fun ResultSet.array(field: String) = getArray(field)
fun ResultSet.asciiStream(field: String) = getAsciiStream(field)
fun ResultSet.bigDecimal(field: String) = getBigDecimal(field)
fun ResultSet.binaryStream(field: String) = getBinaryStream(field)
fun ResultSet.blob(field: String) = getBlob(field)
fun ResultSet.boolean(field: String) = getBoolean(field)
fun ResultSet.byte(field: String) = getByte(field)
fun ResultSet.bytes(field: String) = getBytes(field)
fun ResultSet.characterStream(field: String) = getCharacterStream(field)
fun ResultSet.clob(field: String) = getClob(field)
fun ResultSet.date(field: String) = getDate(field)
fun ResultSet.date(field: String, calendar: Calendar) = getDate(field, calendar)
fun ResultSet.localDate(field: String) = getDate(field).toLocalDate()
fun ResultSet.double(field: String) = getDouble(field)
fun ResultSet.float(field: String) = getFloat(field)
fun ResultSet.int(field: String) = getInt(field)
fun ResultSet.long(field: String) = getLong(field)
fun ResultSet.nCharacterStream(field: String) = getNCharacterStream(field)
fun ResultSet.nClob(field: String) = getNClob(field)
fun ResultSet.nString(field: String) = getNString(field)
inline fun <reified T> ResultSet.t(field: String): T = getObject<T>(field, T::class.java)
fun ResultSet.obj(field: String) = getObject(field)
fun ResultSet.ref(field: String) = getRef(field)
fun ResultSet.rowId(field: String) = getRowId(field)
fun ResultSet.short(field: String) = getShort(field)
fun ResultSet.sqlxml(field: String) = getSQLXML(field)
fun ResultSet.string(field: String) = getString(field)
fun ResultSet.time(field: String) = getTime(field)
fun ResultSet.timestamp(field: String) = getTimestamp(field)
fun ResultSet.instant(field: String) = getTimestamp(field).toInstant()
fun ResultSet.url(field: String) = getURL(field)
