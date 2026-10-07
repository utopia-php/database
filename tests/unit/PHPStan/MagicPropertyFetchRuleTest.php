<?php

namespace Tests\Unit\PHPStan;

use PHPStan\Analyser\Error;
use PHPStan\File\FileHelper;
use PHPStan\Rules\Rule;
use PHPStan\Testing\RuleTestCase;

/**
 * @extends RuleTestCase<MagicPropertyFetchRule>
 */
final class MagicPropertyFetchRuleTest extends RuleTestCase
{
    private const string FIXTURE = __DIR__.'/Data/Fixture.php';

    private const string MAGIC = 'Property Utopia\Database\PDOStatement::$%s is reached through __get/__set/__isset, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use the wrapped \PDOStatement directly instead.';

    private const string HOOKED = 'Property Tests\Unit\PHPStan\Data\Fixture::$%s is reached through its hook, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use a plain property or a method instead.';

    private string $sourceDirectory = __DIR__.'/Data';

    protected function getRule(): Rule
    {
        return new MagicPropertyFetchRule($this->sourceDirectory, self::getContainer()->getByType(FileHelper::class));
    }

    public function testReportsMagicAndHookedPropertyFetchesInTheSourceDirectory(): void
    {
        $this->analyse([self::FIXTURE], [
            [\sprintf(self::HOOKED, 'flag'), 27],
            [\sprintf(self::HOOKED, 'flag'), 32],
            [\sprintf(self::HOOKED, 'counter'), 42],
            [\sprintf(self::HOOKED, 'flag'), 47],
            [\sprintf(self::MAGIC, 'queryString'), 52],
            [\sprintf(self::MAGIC, 'values'), 57],
            [\sprintf(self::MAGIC, 'custom'), 62],
        ]);
    }

    public function testErrorsCarryTheRuleIdentifiers(): void
    {
        $identifiers = \array_map(
            static fn (Error $error): ?string => $error->getIdentifier(),
            $this->gatherAnalyserErrors([self::FIXTURE]),
        );

        $this->assertSame(
            [MagicPropertyFetchRule::HOOKED_IDENTIFIER, MagicPropertyFetchRule::MAGIC_IDENTIFIER],
            \array_values(\array_unique($identifiers)),
        );
    }

    public function testIgnoresFilesOutsideTheSourceDirectory(): void
    {
        $this->sourceDirectory = \sys_get_temp_dir();

        $this->analyse([self::FIXTURE], []);
    }

    public function testRejectsAMissingSourceDirectory(): void
    {
        $missing = __DIR__.'/Data/Missing';

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Source directory "'.$missing.'" does not exist, so the rule would report nothing.');

        new MagicPropertyFetchRule($missing, self::getContainer()->getByType(FileHelper::class));
    }
}
