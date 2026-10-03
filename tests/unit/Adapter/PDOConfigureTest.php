<?php

namespace Tests\Unit\Adapter;

use PDOException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\PDO;

final class PDOConfigureTest extends TestCase
{
    public function testConfigureRejectsAStatementTheEngineRefusesInSilentMode(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null, [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_SILENT]);

        try {
            $pdo->configure('broken', 'NOT SQL');
            $this->fail('A session statement the engine refuses must not be accepted');
        } catch (PDOException $error) {
            $this->assertSame('Failed to configure session: NOT SQL', $error->getMessage());
        }

        $pdo->reconnect();

        $this->assertSame([['value' => 1]], $this->rows($pdo, 'SELECT 1 AS value'));
    }

    public function testConfigureKeepsTheEarlierStatementWhenALaterOneIsRefused(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null, [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_SILENT]);
        $pdo->configure('marker', 'CREATE TEMP TABLE marker AS SELECT 7 AS value');

        try {
            $pdo->configure('marker', 'NOT SQL');
            $this->fail('A session statement the engine refuses must not replace the earlier one');
        } catch (PDOException $error) {
            $this->assertSame('Failed to configure session: NOT SQL', $error->getMessage());
        }

        $pdo->reconnect();

        $this->assertSame([['value' => 7]], $this->rows($pdo, 'SELECT value FROM temp.marker'));
    }

    /**
     * @return array<mixed>
     */
    private function rows(PDO $pdo, string $query): array
    {
        $statement = $pdo->query($query);
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        return $statement->fetchAll(\PDO::FETCH_ASSOC);
    }
}
