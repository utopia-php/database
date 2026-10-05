<?php

namespace Tests\Unit\PHPStan;

use PHPStan\File\FileHelper;
use PHPStan\Rules\Rule;
use PHPStan\Testing\RuleTestCase;

/**
 * @extends RuleTestCase<MagicPropertyFetchRule>
 */
class MagicPropertyFetchRuleTest extends RuleTestCase
{
    private const string MAGIC = 'Magic property %s %s::$%s routes through __get/__set/__isset, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use %s instead.';

    private const string HOOKED = 'Hooked property %s::$%s is accessed through its hook, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use a plain property or a method instead.';

    private const string ATTRIBUTE = 'Utopia\Database\Attribute';

    private const string COLLECTION = 'Utopia\Database\Collection';

    private const string INDEX = 'Utopia\Database\Index';

    private const string RELATIONSHIP = 'Utopia\Database\Relationship';

    private const string STATEMENT = 'Utopia\Database\PDOStatement';

    private const string HOOKED_CLASS = 'Tests\Unit\PHPStan\Data\Source\Hooked';

    private const string KEY_WRITE = "setAttribute('key', \$value)->setAttribute('\$id', \$value)";

    private string $sourceDirectory = __DIR__ . '/Data/Source';

    protected function getRule(): Rule
    {
        return new MagicPropertyFetchRule($this->sourceDirectory, self::getContainer()->getByType(FileHelper::class));
    }

    public function testReportsMagicPropertyFetches(): void
    {
        $this->analyse([__DIR__ . '/Data/Source/magic.php'], [
            [self::read(self::ATTRIBUTE, 'key', 'getKey()'), 19],
            [self::write(self::INDEX, 'ttl', "setAttribute('ttl', \$value)"), 24],
            [self::read(self::RELATIONSHIP, 'twoWay', 'isTwoWay()'), 29],
            [self::read(self::COLLECTION, 'attributes', 'getDeclaredAttributes()'), 34],
            [self::read(self::ATTRIBUTE, 'unknown', "getAttribute('unknown')"), 39],
            [self::read('Utopia\Database\Attribute\Integer', 'size', 'getSize()'), 44],
            [self::read(self::INDEX, 'type', 'getType()'), 49],
            [self::read(self::RELATIONSHIP, 'side', 'getSide()'), 76],
            [self::read(self::STATEMENT, 'queryString', 'the wrapped \PDOStatement directly'), 86],
            [self::read(self::COLLECTION, 'name', 'getName()'), 96],
            [self::read(self::COLLECTION, 'permissions', 'getDeclaredPermissions()'), 101],
            [self::read(self::COLLECTION, 'documentSecurity', 'hasDocumentSecurity()'), 106],
            [self::write(self::ATTRIBUTE, 'key', self::KEY_WRITE), 111],
            [self::write(self::ATTRIBUTE, 'format', "setAttribute('format', \$value === '' ? null : \$value)"), 112],
            [self::write(self::ATTRIBUTE, 'filters', 'setFilters()'), 113],
            [self::write(self::ATTRIBUTE, 'size', "setAttribute('size', \$value)"), 114],
            [self::write(self::INDEX, 'key', self::KEY_WRITE), 119],
            [self::write(self::INDEX, 'type', "setAttribute('type', \$value instanceof IndexType ? \$value->value : \$value)"), 120],
            [self::write(self::INDEX, 'lengths', 'setLengths()'), 121],
            [self::write(self::INDEX, 'orders', 'setOrders()'), 122],
            [self::write(self::INDEX, 'attributes', "setAttribute('attributes', \$value)"), 123],
            [self::write(self::RELATIONSHIP, 'type', "setAttribute('relationType', \$value instanceof RelationType ? \$value->value : \$value)"), 128],
            [self::write(self::RELATIONSHIP, 'key', self::KEY_WRITE), 129],
            [self::write(self::RELATIONSHIP, 'onDelete', "setAttribute('onDelete', \$value instanceof ForeignKeyAction ? \$value->value : \$value)"), 130],
            [self::write(self::RELATIONSHIP, 'side', "setAttribute('side', \$value instanceof RelationSide ? \$value->value : \$value)"), 131],
            [self::write(self::RELATIONSHIP, 'twoWay', "setAttribute('twoWay', \$value)"), 132],
            [self::write(self::COLLECTION, 'id', "setAttribute('\$id', \$value)"), 137],
            [self::write(self::COLLECTION, 'permissions', "setAttribute('\$permissions', \$value ?? [])"), 138],
            [self::write(self::COLLECTION, 'name', "setAttribute('name', \$value)"), 139],
            [self::write(self::STATEMENT, 'custom', 'the wrapped \PDOStatement directly'), 144],
            [self::read(self::ATTRIBUTE, 'key', 'getKey()'), 149],
            [self::read(self::RELATIONSHIP, 'key', 'getKey()'), 154],
            [self::read(self::RELATIONSHIP, 'twoWayKey', 'getTwoWayKey()'), 154],
            [self::write(self::INDEX, 'orders', 'setOrders()'), 159],
        ]);
    }

    public function testReportsHookedPropertyFetchesOutsideTheirOwnHooks(): void
    {
        $this->analyse([__DIR__ . '/Data/Source/Hooked.php'], [
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 25],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 30],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'counter'), 41],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 46],
        ]);
    }

    public function testIgnoresFilesOutsideTheSourceDirectory(): void
    {
        $this->analyse([__DIR__ . '/Data/Outside/magic.php'], []);
    }

    public function testIgnoresEverythingWhenSourceDirectoryDoesNotContainTheFile(): void
    {
        $this->sourceDirectory = __DIR__ . '/Data/Outside';

        $this->analyse([__DIR__ . '/Data/Source/magic.php', __DIR__ . '/Data/Source/Hooked.php'], []);
    }

    public function testResolvesParentSegmentsInTheSourceDirectory(): void
    {
        $this->sourceDirectory = __DIR__ . '/Data/Outside/../Source/';

        $this->analyse([__DIR__ . '/Data/Source/Hooked.php'], [
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 25],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 30],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'counter'), 41],
            [\sprintf(self::HOOKED, self::HOOKED_CLASS, 'flag'), 46],
        ]);
    }

    public function testRejectsMissingSourceDirectory(): void
    {
        $missing = __DIR__ . '/Data/Missing';

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Source directory "' . $missing . '" does not exist, so the rule would report nothing.');

        new MagicPropertyFetchRule($missing, self::getContainer()->getByType(FileHelper::class));
    }

    public function testErrorsCarryTheirIdentifiers(): void
    {
        $identifiers = [];
        foreach ($this->gatherAnalyserErrors([__DIR__ . '/Data/Source/magic.php', __DIR__ . '/Data/Source/Hooked.php']) as $error) {
            $identifiers[] = $error->getIdentifier();
        }

        $identifiers = \array_values(\array_unique($identifiers));
        \sort($identifiers);

        $this->assertSame(
            [MagicPropertyFetchRule::HOOKED_IDENTIFIER, MagicPropertyFetchRule::MAGIC_IDENTIFIER],
            $identifiers,
            'Every reported error must carry one of the rule identifiers',
        );
    }

    private static function read(string $class, string $property, string $replacement): string
    {
        return \sprintf(self::MAGIC, 'fetch of', $class, $property, $replacement);
    }

    private static function write(string $class, string $property, string $replacement): string
    {
        return \sprintf(self::MAGIC, 'write to', $class, $property, $replacement);
    }
}
