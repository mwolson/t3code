// Material's TopAppBar pads its navigation, title and action slots by 4 dp at each edge.
const TOP_APP_BAR_INSETS = 16;
const TITLE_CONTROL_WIDTHS = 2;

export interface AndroidThreadHeaderBudget {
  readonly directActions: number;
  readonly inspectorInOverflow: boolean;
}

// `AndroidScreenHeader` picks direct actions from the header width alone, so a narrow pane at a
// large in-app text size can lose its title. Keeping two `buttonSize` controls' width for the
// title, this folds direct actions, then the inspector toggle, into overflow until the rest
// fit. The pull request control always stays.
export function budgetAndroidThreadHeader(input: {
  readonly headerWidth: number;
  readonly buttonSize: number;
  readonly leadingControls: number;
  readonly actionCount: number;
  readonly pullRequestControls: number;
  readonly hasInspectorToggle: boolean;
}): AndroidThreadHeaderBudget {
  const { actionCount, hasInspectorToggle } = input;
  if (!(input.headerWidth > 0)) {
    return { directActions: actionCount, inspectorInOverflow: false };
  }
  const slots =
    Math.floor((input.headerWidth - TOP_APP_BAR_INSETS) / input.buttonSize) -
    TITLE_CONTROL_WIDTHS -
    input.leadingControls -
    input.pullRequestControls;
  for (const inspectorInOverflow of hasInspectorToggle ? [false, true] : [false]) {
    const inspector = hasInspectorToggle && !inspectorInOverflow ? 1 : 0;
    for (let directActions = actionCount; directActions >= 0; directActions -= 1) {
      const overflow = directActions < actionCount || inspectorInOverflow ? 1 : 0;
      if (directActions + overflow + inspector <= slots) {
        return { directActions, inspectorInOverflow };
      }
    }
  }
  return { directActions: 0, inspectorInOverflow: hasInspectorToggle };
}
