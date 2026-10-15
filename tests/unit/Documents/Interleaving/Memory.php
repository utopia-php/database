<?php

namespace Tests\Unit\Documents\Interleaving;

use Utopia\Database\Adapter\Memory as MemoryAdapter;

final class Memory extends MemoryAdapter
{
    use Interleaves;
}
