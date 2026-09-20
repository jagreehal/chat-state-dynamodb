import { createStoryReporter } from 'executable-stories-vitest/reporter';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    // Every test hits the same DynamoDB Local; run files one at a time so
    // table setup does not race.
    fileParallelism: false,
    testTimeout: 15_000,
    reporters: [
      'default',
      // `reports/test-results.{md,html}` is what executable-stories-action
      // picks up by default in CI.
      createStoryReporter({ formats: ['markdown', 'html'], outputDir: 'reports', outputName: 'test-results' }),
    ],
  },
});
