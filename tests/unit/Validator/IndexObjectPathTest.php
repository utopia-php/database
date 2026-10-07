<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Index;
use Utopia\Database\Validator\IndexDefinition as IndexValidator;

class IndexObjectPathTest extends TestCase
{
    private IndexValidator $validator;

    protected function setUp(): void
    {
        $this->validator = new IndexValidator(
            [
                Attribute::object(key: 'data'),
                Attribute::string(key: 'title', size: 256),
            ],
            [],
            Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::IndexFulltextMultiple, Capability::IndexIdentical, Capability::IndexObject, Capability::IndexTrigram, Capability::IndexKey, Capability::IndexUnique, Capability::IndexFulltext, Capability::Objects], indexLength: 768),
        );
    }

    public function test_fulltext_index_on_object_path_is_rejected(): void
    {
        $this->assertFalse($this->validator->isValid(Index::fulltext(key: 'idx_fulltext', attributes: ['data.title'])));
        $this->assertSame('Attribute "data.title" cannot be part of a fulltext index, must be of type string', $this->validator->getDescription());
    }

    public function test_index_length_on_object_path_is_rejected(): void
    {
        $this->assertFalse($this->validator->isValid(Index::key(key: 'idx_key', attributes: ['data.title'], lengths: [128])));
        $this->assertSame('Cannot set a length on "" attributes', $this->validator->getDescription());
    }

    public function test_trigram_index_on_object_path_is_rejected(): void
    {
        $this->assertFalse($this->validator->isValid(Index::trigram(key: 'idx_trigram', attributes: ['data.title'])));
        $this->assertSame('Trigram index can only be created on string type attributes', $this->validator->getDescription());
    }

    /**
     * @return array<string, array{Index}>
     */
    public static function acceptedIndexes(): array
    {
        return [
            'key on object path' => [Index::key(key: 'idx_key', attributes: ['data.title'])],
            'unique on object path' => [Index::unique(key: 'idx_unique', attributes: ['data.title'])],
            'fulltext on string' => [Index::fulltext(key: 'idx_fulltext', attributes: ['title'])],
            'length on string' => [Index::key(key: 'idx_length', attributes: ['title'], lengths: [128])],
            'trigram on string' => [Index::trigram(key: 'idx_trigram', attributes: ['title'])],
        ];
    }

    #[DataProvider('acceptedIndexes')]
    public function test_indexes_on_known_types_stay_valid(Index $index): void
    {
        $this->assertTrue($this->validator->isValid($index), $this->validator->getDescription());
    }
}
