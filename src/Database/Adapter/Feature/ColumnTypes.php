<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * Provides database-native column type resolution for a database adapter.
 */
interface ColumnTypes
{
    /**
     * Get the expected column type for a given attribute type.
     *
     * @param string $type The attribute type.
     * @param int $size The column size.
     * @param bool $signed Whether the column is signed.
     * @param bool $array Whether the column stores an array.
     * @param bool $required Whether the column is required.
     * @return string The database-native column type string.
     */
    public function getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string;
}
