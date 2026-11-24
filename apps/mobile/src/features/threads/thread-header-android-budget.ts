// Reserve two controls' width for the title and one for navigation.
export function budgetAndroidThreadHeader(input: {
  readonly headerWidth: number;
  readonly buttonSize: number;
  readonly actionCount: number;
  readonly trailingControls: number;
}): number {
  const slots =
    Math.floor((input.headerWidth - 16) / input.buttonSize) - 3 - input.trailingControls;
  return Math.min(input.actionCount, Math.max(0, slots));
}
