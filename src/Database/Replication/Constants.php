<?php

namespace Utopia\Database\Replication;

/**
 * MySQL client/replication protocol constants.
 *
 * Only the subset required to authenticate, request a GTID binlog dump and
 * decode ROW-format events is defined here.
 *
 * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basics.html
 */
final class Constants
{
    // Client capability flags.
    public const int CLIENT_LONG_PASSWORD = 0x00000001;
    public const int CLIENT_LONG_FLAG = 0x00000004;
    public const int CLIENT_CONNECT_WITH_DB = 0x00000008;
    public const int CLIENT_PROTOCOL_41 = 0x00000200;
    public const int CLIENT_SECURE_CONNECTION = 0x00008000;
    public const int CLIENT_PLUGIN_AUTH = 0x00080000;
    public const int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    // Generic packet markers.
    public const int PACKET_OK = 0x00;
    public const int PACKET_EOF = 0xFE;
    public const int PACKET_ERR = 0xFF;
    public const int PACKET_AUTH_MORE_DATA = 0x01;

    // caching_sha2_password fast/full auth markers (inside AuthMoreData).
    public const int AUTH_FAST_SUCCESS = 0x03;
    public const int AUTH_FULL_REQUIRED = 0x04;
    public const int AUTH_REQUEST_PUBLIC_KEY = 0x02;

    // Commands.
    public const int COM_QUERY = 0x03;
    public const int COM_REGISTER_SLAVE = 0x15;
    public const int COM_BINLOG_DUMP_GTID = 0x1E;

    // Binlog event types.
    public const int QUERY_EVENT = 0x02;
    public const int ROTATE_EVENT = 0x04;
    public const int XID_EVENT = 0x10;
    public const int FORMAT_DESCRIPTION_EVENT = 0x0F;
    public const int TABLE_MAP_EVENT = 0x13;
    public const int WRITE_ROWS_EVENT_V1 = 0x17;
    public const int UPDATE_ROWS_EVENT_V1 = 0x18;
    public const int DELETE_ROWS_EVENT_V1 = 0x19;
    public const int HEARTBEAT_EVENT = 0x1B;
    public const int WRITE_ROWS_EVENT_V2 = 0x1E;
    public const int UPDATE_ROWS_EVENT_V2 = 0x1F;
    public const int DELETE_ROWS_EVENT_V2 = 0x20;
    public const int GTID_EVENT = 0x21;
    public const int PREVIOUS_GTIDS_EVENT = 0x23;

    public const int EVENT_HEADER_SIZE = 19;

    // Column types (MYSQL_TYPE_*).
    public const int TYPE_DECIMAL = 0;
    public const int TYPE_TINY = 1;
    public const int TYPE_SHORT = 2;
    public const int TYPE_LONG = 3;
    public const int TYPE_FLOAT = 4;
    public const int TYPE_DOUBLE = 5;
    public const int TYPE_NULL = 6;
    public const int TYPE_TIMESTAMP = 7;
    public const int TYPE_LONGLONG = 8;
    public const int TYPE_INT24 = 9;
    public const int TYPE_DATE = 10;
    public const int TYPE_TIME = 11;
    public const int TYPE_DATETIME = 12;
    public const int TYPE_YEAR = 13;
    public const int TYPE_VARCHAR = 15;
    public const int TYPE_BIT = 16;
    public const int TYPE_TIMESTAMP2 = 17;
    public const int TYPE_DATETIME2 = 18;
    public const int TYPE_TIME2 = 19;
    public const int TYPE_JSON = 245;
    public const int TYPE_NEWDECIMAL = 246;
    public const int TYPE_ENUM = 247;
    public const int TYPE_SET = 248;
    public const int TYPE_TINY_BLOB = 249;
    public const int TYPE_MEDIUM_BLOB = 250;
    public const int TYPE_LONG_BLOB = 251;
    public const int TYPE_BLOB = 252;
    public const int TYPE_VAR_STRING = 253;
    public const int TYPE_STRING = 254;
    public const int TYPE_GEOMETRY = 255;

    // TABLE_MAP optional metadata field types (require binlog_row_metadata=FULL).
    public const int METADATA_COLUMN_NAME = 4;
}
