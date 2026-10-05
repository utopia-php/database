<?php

namespace Tests\Unit\PHPStan;

use PHPStan\Rules\Rule;
use PHPStan\Testing\RuleTestCase;

/**
 * @extends RuleTestCase<MagicPropertyFetchRule>
 */
class MagicPropertyFetchRuleTest extends RuleTestCase
{
    private const string MAGIC = 'Magic property %s %s::$%s routes through __get/__set/__isset, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use %s instead.';

    private const string HOOKED = 'Hooked property %s::$%s is accessed through its hook, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use a plain property or a method instead.';

    private string $sourceDirectory = __DIR__ . '/data/source';

    protected function getRule(): Rule
    {
        return new MagicPropertyFetchRule($this->sourceDirectory);
    }

    public function testReportsMagicPropertyFetches(): void
    {
        $this->analyse([__DIR__ . '/data/source/magic.php'], [
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Attribute', 'key', 'getKey()'), 14],
            [\sprintf(self::MAGIC, 'write to', 'Utopia\Database\Index', 'ttl', 'setAttribute()'), 19],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Relationship', 'twoWay', 'isTwoWay()'), 24],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Collection', 'attributes', 'getDeclaredAttributes()'), 29],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Attribute', 'unknown', "getAttribute('unknown')"), 34],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Attribute\Integer', 'size', 'getSize()'), 39],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Index', 'type', 'getType()'), 44],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\Relationship', 'side', 'getSide()'), 71],
            [\sprintf(self::MAGIC, 'fetch of', 'Utopia\Database\PDOStatement', 'queryString', 'the wrapped \PDOStatement directly'), 81],
        ]);
    }

    public function testReportsHookedPropertyFetchesOutsideTheirOwnHooks(): void
    {
        $this->analyse([__DIR__ . '/data/source/hooked.php'], [
            [\sprintf(self::HOOKED, 'Tests\Unit\PHPStan\Data\Source\Hooked', 'flag'), 25],
            [\sprintf(self::HOOKED, 'Tests\Unit\PHPStan\Data\Source\Hooked', 'flag'), 30],
            [\sprintf(self::HOOKED, 'Tests\Unit\PHPStan\Data\Source\Hooked', 'counter'), 41],
        ]);
    }

    public function testIgnoresFilesOutsideTheSourceDirectory(): void
    {
        $this->analyse([__DIR__ . '/data/outside/magic.php'], []);
    }

    public function testIgnoresEverythingWhenSourceDirectoryDoesNotContainTheFile(): void
    {
        $this->sourceDirectory = __DIR__ . '/data/outside';

        $this->analyse([__DIR__ . '/data/source/magic.php', __DIR__ . '/data/source/hooked.php'], []);
    }

    public function testErrorsCarryTheirIdentifiers(): void
    {
        $identifiers = [];
        foreach ($this->gatherAnalyserErrors([__DIR__ . '/data/source/magic.php', __DIR__ . '/data/source/hooked.php']) as $error) {
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
}
