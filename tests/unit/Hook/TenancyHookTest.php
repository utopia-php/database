<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\RowMetadata;
use Utopia\Database\Hook\Tenancy;
use Utopia\Database\Storage;

final class TenancyHookTest extends TestCase
{
    /**
     * @return array<string, array{int|string|null}>
     */
    public static function tenants(): array
    {
        return [
            'integer' => [5],
            'string' => ['tenant-a'],
            'per document' => [null],
        ];
    }

    #[DataProvider('tenants')]
    public function testDecorateRowStoresTheRowsTenant(int|string|null $tenant): void
    {
        $this->assertSame(
            ['title' => 'x', Storage::TENANT => $tenant],
            (new Tenancy())->decorateRow(['title' => 'x'], new RowMetadata($tenant)),
        );
    }

    public function testDecorateRowStoresTheTenantInTheGivenColumn(): void
    {
        $this->assertSame(['title' => 'x', 'owner' => 5], (new Tenancy('owner'))->decorateRow(['title' => 'x'], new RowMetadata(5)));
    }

    public function testDecorateRowReplacesATenantTheRowAlreadyHolds(): void
    {
        $this->assertSame([Storage::TENANT => 7], (new Tenancy())->decorateRow([Storage::TENANT => 5], new RowMetadata(7)));
    }
}
