<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Storage;

final class StorageTest extends TestCase
{
    public function test_column_mapping(): void
    {
        $this->assertSame(Storage::UID, Storage::column(Document::ID));
        $this->assertSame(Storage::SEQUENCE, Storage::column(Document::SEQUENCE));
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
}
