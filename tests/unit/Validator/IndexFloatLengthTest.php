<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Index;
use Utopia\Database\Validator\Index as IndexValidator;

class IndexFloatLengthTest extends TestCase
{
    private const int MARIADB_MAX_INDEX_LENGTH = 768;

    /**
     * @return array<string, array{Attribute}>
     */
    public static function eightByteNumbers(): array
    {
        return [
            'float' => [Attribute::float(key: 'score')],
            'double' => [Attribute::double(key: 'score')],
        ];
    }

    #[DataProvider('eightByteNumbers')]
    public function test_index_over_the_byte_limit_is_rejected(Attribute $number): void
    {
        $validator = new IndexValidator(
            attributes: [Attribute::string(key: 'title', size: 767), $number],
            indexes: [],
            maxLength: self::MARIADB_MAX_INDEX_LENGTH,
        );

        $this->assertFalse($validator->isValid(Index::key(key: 'idx_title_score', attributes: ['title', 'score'])));
        $this->assertSame('Index length is longer than the maximum: 768', $validator->getDescription());
    }

    #[DataProvider('eightByteNumbers')]
    public function test_index_at_the_byte_limit_is_valid(Attribute $number): void
    {
        $validator = new IndexValidator(
            attributes: [Attribute::string(key: 'title', size: 766), $number],
            indexes: [],
            maxLength: self::MARIADB_MAX_INDEX_LENGTH,
        );

        $this->assertTrue($validator->isValid(Index::key(key: 'idx_title_score', attributes: ['title', 'score'])), $validator->getDescription());
    }
}
