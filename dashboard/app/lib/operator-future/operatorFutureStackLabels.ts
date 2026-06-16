export function formatStrategyAxisLabel(value: string): string {
  const wrappedLabels: Record<string, string> = {
    'Best valid': 'Best\nvalid',
    'DT Shadow': 'DT\nShadow',
    'Direct DT': 'Direct\nDT',
    'DT/V2+': 'DT/V2+',
    'DT distill': 'DT\ndistill',
    'Decision DT': 'Decision\nDT',
    'RA V2+': 'RA\nV2+',
    'DT V2+ safe-switch': 'DT V2+\nsafe-switch',
    'HF live safe-switch': 'HF live\nsafe-switch',
    'HF value-aligned': 'HF value\naligned',
    'Poland/TFT': 'Poland\nTFT',
    'DFL diag': 'DFL\ndiag',
    'V13 blocked': 'V13\nblocked'
  }

  return wrappedLabels[value] ?? value
}

export const formatHour = (timestamp: string): string => new Date(timestamp)
  .toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })
