<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\SetType;

final class DocumentsValidatorFingerprintTest extends TestCase
{
    public function testAnEqualSchemaReusesTheValidator(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));

        $this->assertSame(
            $database->documentsValidator($this->collection()),
            $database->documentsValidator($this->collection()),
        );
        $this->assertSame(
            $database->documentsValidator($this->collection()),
            $database->documentsValidator(clone $this->collection()),
        );
    }

    public function testEveryPartOfTheSchemaSelectsItsOwnValidator(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));
        $original = $database->documentsValidator($this->collection());

        $renamed = $this->collection();
        $renamed->setAttribute('attributes', [Attribute::string(key: 'author', size: 64)->toDocument()]);
        $resized = $this->collection();
        $resized->setAttribute('attributes', [Attribute::string(key: 'title', size: 32)->toDocument()]);
        $added = $this->collection();
        $added->setAttribute('attributes', Attribute::integer(key: 'pages')->toDocument(), SetType::Append);
        $indexed = $this->collection();
        $indexed->setAttribute('indexes', []);
        $permitted = $this->collection();
        $permitted->setAttribute('$permissions', [Permission::read(Role::users())]);
        $secured = $this->collection();
        $secured->setAttribute('documentSecurity', true);

        foreach (['renamed' => $renamed, 'resized' => $resized, 'added' => $added, 'indexed' => $indexed, 'permitted' => $permitted, 'secured' => $secured] as $change => $collection) {
            $this->assertNotSame($original, $database->documentsValidator($collection), $change);
        }

        $this->assertTrue($database->documentsValidator($renamed)->isValid([Query::equal('author', ['ada'])]));
        $this->assertFalse($database->documentsValidator($renamed)->isValid([Query::equal('title', ['ada'])]));
        $this->assertTrue($database->documentsValidator($added)->isValid([Query::equal('pages', [1])]));
        $this->assertFalse($database->documentsValidator($this->collection())->isValid([Query::equal('pages', [1])]));
    }

    private function collection(): Collection
    {
        return Collection::create(
            id: 'books',
            attributes: [Attribute::string(key: 'title', size: 64)],
            indexes: [Index::key(key: 'by_title', attributes: ['title'])],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );
    }
}
