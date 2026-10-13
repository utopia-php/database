<?php

namespace Tests\Unit\Support;

/**
 * The engine family whose 7.4.1 arithmetic a fixture row records: MariaDB also stands for MySQL, and Memory for
 * Redis and SQLite.
 */
enum FractionalEngine
{
    case MariaDB;
    case PostgreSQL;
    case MongoDB;
    case Memory;
}
