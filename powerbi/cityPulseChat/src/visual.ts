"use strict";

import powerbi from "powerbi-visuals-api";
import { ChatDialog, ChatDialogState } from "./chatDialog";
import "../style/visual.less";

import IVisual = powerbi.extensibility.visual.IVisual;
import IVisualHost = powerbi.extensibility.visual.IVisualHost;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function reportContext(options: VisualUpdateOptions): Pick<ChatDialogState, "month" | "borough" | "scopeNote"> {
    const table = options.dataViews?.[0]?.table;
    if (!table) {
        return { month: null, borough: null, scopeNote: "Ask about the full published snapshot, or name a month or borough." };
    }
    const monthIndex = table.columns.findIndex(column => column.roles?.reportMonth);
    const boroughIndex = table.columns.findIndex(column => column.roles?.reportBorough);
    const unique = (index: number): string[] => index < 0 ? [] : [...new Set(
        table.rows.map(row => String(row[index] ?? "").trim()).filter(Boolean)
    )];
    const monthValues = unique(monthIndex);
    const boroughValues = unique(boroughIndex);
    let month: string | null = null;
    if (monthValues.length === 1) {
        const value = monthValues[0];
        const match = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})$/i.exec(value);
        if (match) {
            month = `${match[2]}-${String(MONTHS.findIndex(item => item.toLowerCase() === match[1].toLowerCase()) + 1).padStart(2, "0")}`;
        } else if (/^20\d{2}-(0[1-9]|1[0-2])$/.test(value)) {
            month = value;
        }
    }
    const borough = boroughValues.length === 1 ? boroughValues[0] : null;
    const caveats: string[] = [];
    if (monthValues.length > 1) caveats.push("More than one month is visible; name a month in your question for an exact match.");
    if (boroughValues.length > 1) caveats.push("More than one borough is visible; name a borough for an exact match.");
    if (monthValues.length === 1 && !month) caveats.push("The report month could not be read; name it in your question.");
    const selected = [month, borough].filter(Boolean).join(" · ");
    return { month, borough, scopeNote: caveats.join(" ") || (selected ? `Report context: ${selected}` : "Full published snapshot") };
}

export class Visual implements IVisual {
    private readonly host: IVisualHost;
    private readonly target: HTMLElement;
    private readonly note: HTMLElement;
    private context: Pick<ChatDialogState, "month" | "borough" | "scopeNote"> = {
        month: null, borough: null, scopeNote: "Full published snapshot",
    };

    constructor(options: VisualConstructorOptions) {
        this.host = options.host;
        this.target = options.element;
        this.target.classList.add("city-pulse-launcher");
        const button = document.createElement("button");
        button.type = "button";
        button.className = "city-pulse-button";
        button.setAttribute("aria-label", "Open City Pulse chat");
        button.title = "Ask about NYC taxi data";
        const icon = document.createElement("span");
        icon.className = "city-pulse-icon";
        icon.setAttribute("aria-hidden", "true");
        icon.textContent = "✦";
        const label = document.createElement("span");
        label.textContent = "Ask City Pulse";
        button.append(icon, label);
        button.addEventListener("click", () => this.openChat());
        this.note = document.createElement("span");
        this.note.className = "city-pulse-launcher-note";
        this.target.append(button, this.note);
    }

    public update(options: VisualUpdateOptions): void {
        this.host.eventService?.renderingStarted(options);
        try {
            this.context = reportContext(options);
            this.host.eventService?.renderingFinished(options);
        } catch (error) {
            this.host.eventService?.renderingFailed(options, String(error));
        }
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        // An empty visual-specific card list still enables Power BI's General pane,
        // including title, position, size, and background controls.
        return { cards: [] };
    }

    private openChat(): void {
        if (!this.host.hostCapabilities?.allowModalDialog) {
            this.note.textContent = "Chat pop-ups are disabled in this Power BI environment.";
            return;
        }
        this.note.textContent = "";
        const state: ChatDialogState = { ...this.context };
        this.host.openModalDialog(ChatDialog.id, {
            // Power BI already shows the visual's display name above this title.
            title: "",
            size: { width: 560, height: 530 },
            actionButtons: [],
        }, state).catch(() => {
            this.note.textContent = "Power BI could not open chat. Check custom-visual permissions.";
        });
    }
}
