import * as React from "react";
import { Checkbox } from "@blueprintjs/core";
import { TextFormatOptions } from "../TextFormatOptions";

export interface TextFormatPanelProps {
  opts: TextFormatOptions;
  onChange?: (opts: TextFormatOptions) => void;
}

export const TextFormatPanel: React.FC<TextFormatPanelProps> = () => {
  return (
    <div className="format-subpanel">
      <Checkbox
        className="bp4-condensed"
        checked={false}
        disabled={true}
        label="Render URLs as Hyperlinks (Disabled — Plain-Text Safe Mode)"
      />
    </div>
  );
};
