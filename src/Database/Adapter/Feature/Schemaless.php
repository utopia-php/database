<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * An adapter that can store documents without a declared schema: in schemaless mode it accepts attributes
 * the collection does not define and validates none of them.
 */
interface Schemaless
{
    public function setSchemaless(bool $schemaless): static;

    public function isSchemaless(): bool;
}
