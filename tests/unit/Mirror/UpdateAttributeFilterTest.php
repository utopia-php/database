<?php

namespace Tests\Unit\Mirror;

use ArrayObject;
use Closure;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Mirror;
use Utopia\Database\Mirror\Failure;
use Utopia\Database\Mirror\Filter;

final class UpdateAttributeFilterTest extends TestCase
{
    public function testAFilterThatChangesTheAttributeInPlaceIsReplicated(): void
    {
        $destination = HookFixture::memory();
        $mirror = $this->mirror($destination, static function (Document $attribute): Document {
            $attribute->setAttribute('default', 'mirrored');

            return $attribute;
        });
        $errors = $this->errors($mirror);

        $mirror->updateAttribute(HookFixture::COLLECTION, 'title', new AttributeUpdate(default: 'source'));

        $this->assertSame([], $errors->getArrayCopy());
        $this->assertSame('source', $this->stored($mirror->getSource(), 'title')->default);
        $this->assertSame('mirrored', $this->stored($destination, 'title')->default);
    }

    public function testAFilteredUpdateDoesNotRewriteAnUnchangedColumn(): void
    {
        $destination = HookFixture::database(new class () extends Memory {
            #[\Override]
            public function updateAttribute(string $collection, string $key, Attribute $attribute): bool
            {
                throw new DatabaseException('Column rewrite refused');
            }
        });
        $mirror = $this->mirror($destination, static function (Document $attribute): Document {
            $filtered = clone $attribute;
            $filtered->setAttribute('default', 'mirrored');

            return $filtered;
        });
        $errors = $this->errors($mirror);

        $mirror->updateAttribute(HookFixture::COLLECTION, 'title', new AttributeUpdate(default: 'source', required: false));

        $this->assertSame([], $errors->getArrayCopy());
        $this->assertSame('mirrored', $this->stored($destination, 'title')->default);
        $this->assertSame(64, $this->stored($destination, 'title')->size);
    }

    public function testAFilterThatChangesAFieldTheUpdateLeftAloneReplicatesIt(): void
    {
        $destination = HookFixture::memory();
        $mirror = $this->mirror($destination, static function (Document $attribute): Document {
            $attribute->setAttribute('size', 128);

            return $attribute;
        });
        $errors = $this->errors($mirror);

        $mirror->updateAttribute(HookFixture::COLLECTION, 'title', new AttributeUpdate(default: 'source'));

        $this->assertSame([], $errors->getArrayCopy());
        $this->assertSame(64, $this->stored($mirror->getSource(), 'title')->size);
        $this->assertSame(128, $this->stored($destination, 'title')->size);
        $this->assertSame('source', $this->stored($destination, 'title')->default);
    }

    /**
     * @param  Closure(Document): Document  $transform
     */
    private function mirror(Database $destination, Closure $transform): Mirror
    {
        return new Mirror(HookFixture::memory(), $destination, [new class ($transform) extends Filter {
            /**
             * @param  Closure(Document): Document  $transform
             */
            public function __construct(private readonly Closure $transform)
            {
            }

            #[\Override]
            public function beforeUpdateAttribute(Database $source, Database $destination, string $collectionId, string $attributeId, ?Document $attribute = null): ?Document
            {
                return $attribute === null ? null : ($this->transform)($attribute);
            }
        }]);
    }

    /**
     * @return ArrayObject<int, string>
     */
    private function errors(Mirror $mirror): ArrayObject
    {
        /** @var ArrayObject<int, string> $errors */
        $errors = new ArrayObject();
        $mirror->onError(static function (Failure $failure) use ($errors): void {
            $errors[] = $failure->method.': '.$failure->error->getMessage();
        });

        return $errors;
    }

    private function stored(Database $database, string $key): Attribute
    {
        foreach ($database->getCollection(HookFixture::COLLECTION)->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Attribute '.$key.' is missing from the collection metadata');
    }
}
