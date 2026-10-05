<?php

namespace Tests\Unit\Support;

/**
 * What a scripted commitTransaction does on the server in MongoCommitRetryTest.
 */
enum CommitOutcome
{
    case Committed;

    case AppliedThenLost;

    case NotApplied;

    case Aborted;
}
