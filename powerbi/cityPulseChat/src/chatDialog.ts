"use strict";

import powerbi from "powerbi-visuals-api";

import DialogConstructorOptions = powerbi.extensibility.visual.DialogConstructorOptions;

export interface ChatDialogState {
    month: string | null;
    borough: string | null;
    scopeNote: string;
}

interface ChatResponse {
    answer?: string;
    session_id?: string;
    ai_used?: boolean;
    error?: string;
}

interface HealthResponse {
    status?: string;
    auth_required?: boolean;
    missing_exports?: string[];
}

function element<K extends keyof HTMLElementTagNameMap>(tag: K, className: string, text?: string): HTMLElementTagNameMap[K] {
    const node = document.createElement(tag);
    node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
}

export class ChatDialog {
    public static readonly id = "CityPulseChatDialog";
    private static accessCode = "";
    private readonly state: ChatDialogState;
    private readonly messages: HTMLElement;
    private readonly authorization: HTMLElement;
    private readonly codeInput: HTMLInputElement;
    private readonly input: HTMLInputElement;
    private readonly send: HTMLButtonElement;
    private readonly status: HTMLElement;
    private sessionId: string | null = null;
    private authRequired = false;

    constructor(options: DialogConstructorOptions, initialState: ChatDialogState) {
        this.state = initialState || { month: null, borough: null, scopeNote: "Full published snapshot" };
        const root = element("section", "city-pulse-chat");
        const header = element("div", "city-pulse-chat-header");
        header.append(element("div", "city-pulse-chat-kicker", "NYC YELLOW TAXI  /  DATA ASSISTANT"));
        header.append(element("h2", "city-pulse-chat-title", "Ask the city."));
        header.append(element("p", "city-pulse-chat-subtitle", "Answers grounded in the report's published data."));
        const scope = element("div", "city-pulse-chat-scope", this.state.scopeNote);
        this.authorization = element("div", "city-pulse-chat-auth");
        this.authorization.hidden = true;
        const codeLabel = element("label", "city-pulse-chat-auth-label", "🔐 LOCAL ACCESS CODE");
        this.codeInput = element("input", "city-pulse-chat-auth-input");
        this.codeInput.type = "password";
        this.codeInput.autocomplete = "off";
        this.codeInput.placeholder = "Paste code shown by the start script";
        this.codeInput.value = ChatDialog.accessCode;
        codeLabel.append(this.codeInput);
        this.authorization.append(codeLabel);
        this.messages = element("div", "city-pulse-chat-messages");
        this.messages.setAttribute("role", "log");
        this.messages.setAttribute("aria-live", "polite");
        this.addMessage("assistant", "Hi! Ask me about taxi trips, busiest hours, pickup boroughs, payment methods, or month-to-month changes.");
        const suggestions = element("div", "city-pulse-chat-suggestions");
        for (const question of ["Which month was busiest?", "What was the top borough?", "Which payment method led?"]) {
            const chip = element("button", "city-pulse-chat-chip", question);
            chip.setAttribute("type", "button");
            chip.addEventListener("click", () => this.ask(question));
            suggestions.append(chip);
        }
        const composer = element("div", "city-pulse-chat-form");
        this.input = element("input", "city-pulse-chat-input");
        this.input.type = "text";
        this.input.maxLength = 500;
        this.input.autocomplete = "off";
        this.input.placeholder = "Ask about the taxi data…";
        this.input.setAttribute("aria-label", "Question about taxi data");
        this.send = element("button", "city-pulse-chat-send", "Send");
        this.send.type = "button";
        composer.append(this.input, this.send);
        const submitQuestion = (): void => { void this.ask(this.input.value); };
        this.send.addEventListener("click", submitQuestion);
        this.input.addEventListener("keydown", event => {
            if (event.key !== "Enter") return;
            event.preventDefault();
            event.stopPropagation();
            submitQuestion();
        });
        this.status = element("p", "city-pulse-chat-status", "Local data assistant · answers checked against published data");
        root.append(header, scope, this.authorization, this.messages, suggestions, composer, this.status);
        options.element.append(root);
        void this.checkService();
    }

    private async checkService(): Promise<void> {
        try {
            // eslint-disable-next-line powerbi-visuals/no-http-string
            const response = await fetch("http://127.0.0.1:8765/health");
            const health = await response.json() as HealthResponse;
            this.authRequired = health.auth_required === true;
            this.authorization.hidden = !this.authRequired;
            if (health.status !== "ready") {
                this.status.textContent = `Missing chat CSV exports: ${(health.missing_exports || []).join(", ")}`;
            } else if (this.authRequired) {
                this.status.textContent = "Protected local service · paste its current access code";
            } else {
                this.status.textContent = "Connected to local taxi data · no access code needed";
            }
        } catch {
            this.status.textContent = "Open powerbi/Open City Pulse.cmd to connect the local data service";
        }
    }

    private addMessage(role: "user" | "assistant", text: string): void {
        const bubble = element("div", `city-pulse-message city-pulse-message-${role}`);
        bubble.append(element("div", "city-pulse-message-body", text));
        this.messages.append(bubble);
        this.messages.scrollTop = this.messages.scrollHeight;
    }

    private async ask(raw: string): Promise<void> {
        const question = raw.trim();
        if (!question || this.send.disabled) return;
        const accessCode = this.authRequired ? this.codeInput.value.trim() : "";
        if (this.authRequired && !accessCode) {
            this.status.textContent = "Paste the access code displayed when the local chat service starts.";
            this.codeInput.focus();
            return;
        }
        if (accessCode) ChatDialog.accessCode = accessCode;
        this.input.value = "";
        this.addMessage("user", question);
        this.send.disabled = true;
        this.status.textContent = "Checking published taxi data…";
        try {
            // Desktop talks only to the machine-local, read-only data service.
            const headers: Record<string, string> = { "Content-Type": "application/json" };
            if (accessCode) headers["X-City-Pulse-Code"] = accessCode;
            // eslint-disable-next-line powerbi-visuals/no-http-string
            const response = await fetch("http://127.0.0.1:8765/chat", {
                method: "POST",
                headers,
                body: JSON.stringify({ question, month: this.state.month, borough: this.state.borough, session_id: this.sessionId }),
            });
            const data = await response.json() as ChatResponse;
            if (!response.ok || data.error) throw new Error(data.error || `HTTP ${response.status}`);
            this.sessionId = data.session_id || this.sessionId;
            this.addMessage("assistant", data.answer || "No answer was returned.");
            this.status.textContent = data.ai_used ? "Local AI interpretation · figures from published marts" : "Verified report-data answer · local AI optional";
        } catch (error) {
            const reason = error instanceof Error ? error.message : String(error);
            if (/access code|HTTP 401/i.test(reason)) {
                this.authRequired = true;
                this.authorization.hidden = false;
                ChatDialog.accessCode = "";
                this.codeInput.value = "";
                this.addMessage("assistant", "That access code was not accepted. Copy the current code from the local chat service terminal and try again.");
                this.status.textContent = "Access code required";
                this.codeInput.focus();
            } else {
                this.addMessage("assistant", "I couldn't reach the local taxi data service. Open powerbi/Open City Pulse.cmd and try again.");
                this.status.textContent = `Connection error: ${reason}`;
            }
        } finally {
            this.send.disabled = false;
            if (this.authRequired && !this.codeInput.value) this.codeInput.focus();
            else this.input.focus();
        }
    }
}

type DialogRegistry = typeof globalThis & { dialogRegistry?: Record<string, typeof ChatDialog> };
const registry = globalThis as DialogRegistry;
registry.dialogRegistry = registry.dialogRegistry || {};
registry.dialogRegistry[ChatDialog.id] = ChatDialog;
