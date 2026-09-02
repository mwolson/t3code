export interface CollapsedComposerActionsInput {
  readonly canStopThread: boolean;
  readonly hasContent: boolean;
}

export interface CollapsedComposerActions {
  readonly showStopPrimary: boolean;
}

export function collapsedComposerActions(
  input: CollapsedComposerActionsInput,
): CollapsedComposerActions {
  return {
    showStopPrimary: input.canStopThread && !input.hasContent,
  };
}
