import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: { index: 'src/index.ts' },
  format: ['esm'],
  dts: true,
  clean: true,
  deps: { neverBundle: ['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb', 'chat'] },
});
