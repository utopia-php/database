<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Storage;

final class StorageTest extends TestCase
{
    public function test_document_reserved_keys(): void
    {
        $this->assertSame('$id', Document::ID);
        $this->assertSame('$sequence', Document::SEQUENCE);
        $this->assertSame('$collection', Document::COLLECTION);
        $this->assertSame('$createdAt', Document::CREATED_AT);
        $this->assertSame('$updatedAt', Document::UPDATED_AT);
        $this->assertSame('$permissions', Document::PERMISSIONS);
        $this->assertSame('$tenant', Document::TENANT);
        $this->assertSame('$version', Document::VERSION);
        $this->assertSame('$distance', Document::DISTANCE);
        $this->assertSame('$deletedAt', Document::DELETED_AT);
        $this->assertSame('$skipPermissionsUpdate', Document::SKIP_PERMISSIONS_UPDATE);
        $this->assertSame('$internalId', Document::INTERNAL_ID);
    }

    public function test_column_mapping(): void
    {
        $this->assertSame(Storage::UID, Storage::column(Document::ID));
        $this->assertSame(Storage::SEQUENCE, Storage::column(Document::SEQUENCE));
        $this->assertSame('_id', Storage::SEQUENCE);
        $this->assertSame(Document::INTERNAL_ID, Storage::column(Document::INTERNAL_ID));
        $this->assertSame('title', Storage::column('title'));
        $this->assertFalse(\defined(Storage::class.'::ID'));
    }

    public function test_maps_invert(): void
    {
        foreach (Storage::attributeMap() as $attribute => $column) {
            $this->assertSame($attribute, Storage::attribute($column));
        }
    }

    public function test_permissions_table(): void
    {
        $this->assertSame('movies_perms', Storage::permissionsTable('movies'));
    }

    public function test_vector_distance(): void
    {
        $this->assertSame(Document::DISTANCE, Database::VECTOR_DISTANCE);
        $this->assertSame('$distance', Document::DISTANCE);
    }

    public function test_internal_attribute_keys(): void
    {
        $this->assertSame([
            '_uid',
            '_createdAt',
            '_updatedAt',
            '_permissions',
            '_version',
        ], Database::INTERNAL_ATTRIBUTE_KEYS);
        $this->assertSame(5, \count(Database::INTERNAL_ATTRIBUTE_KEYS));
    }

    public function test_internal_indexes(): void
    {
        $this->assertSame([
            '_id',
            '_uid',
            '_createdAt',
            '_updatedAt',
            '_permissions_id',
            '_permissions',
        ], Database::INTERNAL_INDEXES);
        $this->assertSame(6, \count(Database::INTERNAL_INDEXES));
    }
}
