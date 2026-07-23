// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Tencent. All rights reserved.

import { useEffect, useRef, useState, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import { WebLinksAddon } from '@xterm/addon-web-links';
import { Unicode11Addon } from '@xterm/addon-unicode11';
import { X, Minimize2, Maximize2, Copy, Check, RefreshCw } from 'lucide-react';
import { Rnd } from 'react-rnd';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { sandboxApi, type SandboxDetail } from '@/api/client';
import '@xterm/xterm/css/xterm.css';

interface TerminalPanelProps {
  sandboxId: string;
  onClose: () => void;
}

interface PanelRect {
  width: number;
  height: number;
  left: number;
  top: number;
}

interface TerminalContainer {
  name: string;
  containerID: string;
  status: string;
  image: string;
  kind?: string | null;
  primary: boolean;
}

type SandboxDetailWithContainers = SandboxDetail & {
  containers?: TerminalContainer[];
};

const PANEL_MARGIN = 24;
const PANEL_MIN_WIDTH = 720;
const PANEL_MIN_HEIGHT = 420;

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function createInitialPanelRect(): PanelRect {
  if (typeof window === 'undefined') {
    return { width: 1100, height: 720, left: 0, top: 0 };
  }

  const maxWidth = Math.max(window.innerWidth - PANEL_MARGIN * 2, PANEL_MIN_WIDTH);
  const maxHeight = Math.max(window.innerHeight - PANEL_MARGIN * 2, PANEL_MIN_HEIGHT);
  const width = clamp(Math.round(window.innerWidth * 0.8), PANEL_MIN_WIDTH, maxWidth);
  const height = clamp(Math.round(window.innerHeight * 0.8), PANEL_MIN_HEIGHT, maxHeight);

  return {
    width,
    height,
    left: Math.round((window.innerWidth - width) / 2),
    top: Math.round((window.innerHeight - height) / 2),
  };
}

function clampPanelRect(rect: PanelRect): PanelRect {
  if (typeof window === 'undefined') return rect;

  const maxWidth = Math.max(window.innerWidth - PANEL_MARGIN * 2, PANEL_MIN_WIDTH);
  const maxHeight = Math.max(window.innerHeight - PANEL_MARGIN * 2, PANEL_MIN_HEIGHT);
  const width = clamp(rect.width, PANEL_MIN_WIDTH, maxWidth);
  const height = clamp(rect.height, PANEL_MIN_HEIGHT, maxHeight);
  const left = clamp(rect.left, PANEL_MARGIN, Math.max(PANEL_MARGIN, window.innerWidth - width - PANEL_MARGIN));
  const top = clamp(rect.top, PANEL_MARGIN, Math.max(PANEL_MARGIN, window.innerHeight - height - PANEL_MARGIN));

  return { width, height, left, top };
}

function base64UrlEncode(value: string) {
  const bytes = new TextEncoder().encode(value);
  let binary = '';
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return window.btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function resolveDefaultContainerId(containers: TerminalContainer[]) {
  if (containers.length === 1) {
    return containers[0].containerID;
  }

  const primaryContainers = containers.filter((container) => container.primary);
  return primaryContainers.length === 1 ? primaryContainers[0].containerID : null;
}

export function TerminalPanel({ sandboxId, onClose }: TerminalPanelProps) {
  const { t } = useTranslation('sandboxes');
  const sandboxQuery = useQuery({
    queryKey: ['sandbox-terminal', sandboxId],
    queryFn: () => sandboxApi.get(sandboxId),
    enabled: !!sandboxId,
    staleTime: 30_000,
  });
  const terminalRef = useRef<HTMLDivElement>(null);
  const termRef = useRef<Terminal | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const resizeFrameRef = useRef<number | null>(null);
  const isResizingPanelRef = useRef(false);
  const lastSentSizeRef = useRef<{ rows: number; cols: number } | null>(null);
  const lastWindowedRectRef = useRef<PanelRect | null>(null);
  const selectedSandboxIdRef = useRef(sandboxId);
  const [isConnected, setIsConnected] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(true);
  const [panelRect, setPanelRect] = useState<PanelRect>(() => createInitialPanelRect());
  const [copied, setCopied] = useState(false);
  const [connectionKey, setConnectionKey] = useState(0);
  const [canReconnect, setCanReconnect] = useState(false);
  const [statusText, setStatusText] = useState(t('terminal.connecting'));
  const [fontSize, setFontSize] = useState(14);
  const [selectedContainerId, setSelectedContainerId] = useState<string | null>(null);
  const accessToken = localStorage.getItem('cube.accessToken')?.trim() ?? '';
  const apiKey = localStorage.getItem('cube.apiKey')?.trim() ?? '';
  const credential = accessToken || apiKey;
  const credentialParam = accessToken ? 'accessTokenB64' : 'apiKeyB64';
  const canConnect = credential.length > 0;
  const detail = sandboxQuery.data as SandboxDetailWithContainers | undefined;
  const containers = detail?.containers ?? [];
  const selectedContainer = containers.find((item) => item.containerID === selectedContainerId) ?? null;
  const defaultContainerId = resolveDefaultContainerId(containers);

  useEffect(() => {
    const sandboxChanged = selectedSandboxIdRef.current !== sandboxId;
    selectedSandboxIdRef.current = sandboxId;
    setSelectedContainerId((current) =>
      sandboxChanged ? defaultContainerId : current ?? defaultContainerId,
    );
  }, [defaultContainerId, sandboxId]);

  useEffect(() => {
    if (!credential) {
      setStatusText(t('terminal.apiKeyMissing'));
      return;
    }
    if (sandboxQuery.isLoading) {
      setStatusText(t('terminal.loadingSandbox'));
      return;
    }
    if (containers.length > 1 && !selectedContainerId) {
      setStatusText(t('terminal.selectContainer'));
      return;
    }
    if (selectedContainerId) {
      setStatusText(t('terminal.connecting'));
    }
  }, [
    credential,
    containers.length,
    sandboxQuery.isLoading,
    selectedContainerId,
    t,
  ]);

  const writeStatusLine = useCallback((message: string, color = '33') => {
    termRef.current?.write(`\r\n\x1b[1;${color}m${message}\x1b[0m\r\n`);
  }, []);

  const handleCopy = useCallback(async () => {
    if (termRef.current) {
      const selection = termRef.current.getSelection();
      if (selection) {
        await navigator.clipboard.writeText(selection);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      }
    }
  }, []);

  const handlePaste = useCallback(async () => {
    if (termRef.current && wsRef.current?.readyState === WebSocket.OPEN) {
      try {
        const text = await navigator.clipboard.readText();
        wsRef.current.send(text);
      } catch (err) {
        console.error('Paste failed:', err);
      }
    }
  }, []);

  useEffect(() => {
    if (!terminalRef.current || !selectedContainerId || !canConnect) return;
    const terminalElement = terminalRef.current;
    setCanReconnect(false);
    setStatusText(t('terminal.connecting'));
    lastSentSizeRef.current = null;

    const term = new Terminal({
      cursorBlink: true,
      cursorStyle: 'bar',
      cursorWidth: 2,
      scrollback: 10000,
      scrollSensitivity: 1,
      tabStopWidth: 8,
      fontSize: 14,
      fontFamily: 'Monaco, Menlo, "Ubuntu Mono", "Consolas", "Source Code Pro", monospace',
      fontWeight: 'normal',
      fontWeightBold: 'bold',
      lineHeight: 1.4,
      letterSpacing: 0,
      convertEol: true,
      disableStdin: false,
      screenReaderMode: false,
      allowTransparency: false,
      allowProposedApi: true,
      theme: {
        background: '#0d1117',
        foreground: '#c9d1d9',
        cursor: '#58a6ff',
        cursorAccent: '#0d1117',
        selectionBackground: '#264f78',
        black: '#161b22',
        red: '#f85149',
        green: '#3fb950',
        yellow: '#d29922',
        blue: '#58a6ff',
        magenta: '#bc8cff',
        cyan: '#39d353',
        white: '#c9d1d9',
        brightBlack: '#6e7681',
        brightRed: '#ff7b72',
        brightGreen: '#56d364',
        brightYellow: '#e3b341',
        brightBlue: '#79c0ff',
        brightMagenta: '#d2a8ff',
        brightCyan: '#56d4dd',
        brightWhite: '#f0f6fc',
      },
    });
    termRef.current = term;

    const fitAddon = new FitAddon();
    fitAddonRef.current = fitAddon;
    term.loadAddon(fitAddon);

    const webLinksAddon = new WebLinksAddon();
    term.loadAddon(webLinksAddon);

    const unicode11Addon = new Unicode11Addon();
    term.loadAddon(unicode11Addon);

    term.write(`\x1b[2J\x1b[H${t('terminal.welcome')}\r\n`);
    term.write(`\x1b[1;34m${t('terminal.sandbox', { sandboxId })}\x1b[0m\r\n\r\n`);

    term.open(terminalElement);
    fitAddon.fit();

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const authQuery = `&${credentialParam}=${encodeURIComponent(base64UrlEncode(credential))}`;
    const wsUrl = `${protocol}//${window.location.host}/sandboxes/${sandboxId}/terminal?container=${encodeURIComponent(selectedContainerId)}${authQuery}`;
    const ws = new WebSocket(wsUrl);
    let disposed = false;
    let opened = false;
    let failed = false;
    const isCurrentSocket = () => !disposed && wsRef.current === ws;
    wsRef.current = ws;

    ws.onopen = () => {
      if (!isCurrentSocket()) return;
      opened = true;
      setIsConnected(true);
      setCanReconnect(false);
      setStatusText(t('terminal.connected'));
      fitAddon.fit();
      const currentSize = { rows: term.rows, cols: term.cols };
      lastSentSizeRef.current = currentSize;
      ws.send(JSON.stringify({ type: 'resize', ...currentSize }));
      term.focus();
    };

    ws.onmessage = (event) => {
      if (!isCurrentSocket()) return;
      if (typeof event.data !== 'string') return;
      try {
        const message = JSON.parse(event.data) as {
          type?: string;
          data?: string;
          code?: string;
          message?: string;
          reason?: string;
        };
        if (message.type === 'output') {
          term.write(message.data ?? '');
          return;
        }
        if (message.type === 'ready') {
          setStatusText(t('terminal.connected'));
          return;
        }
        if (message.type === 'error') {
          failed = true;
          const detail = message.message || message.code || t('terminal.connectionFailed');
          setStatusText(t('terminal.failed'));
          writeStatusLine(`${t('terminal.error')}: ${detail}`, '31');
          return;
        }
        if (message.type === 'closed') {
          const detail = message.message || message.reason || t('terminal.closed');
          setStatusText(t('terminal.disconnected'));
          writeStatusLine(detail, '33');
          return;
        }
      } catch {
        term.write(event.data);
      }
    };

    ws.onerror = () => {
      if (!isCurrentSocket()) return;
      failed = true;
      console.warn('[terminal] websocket error', {
        sandboxId,
        containerId: selectedContainerId,
      });
      setStatusText(t('terminal.failed'));
      writeStatusLine(t('terminal.connectionFailed'), '31');
    };

    ws.onclose = (event) => {
      if (!isCurrentSocket()) return;
      setIsConnected(false);
      setCanReconnect(true);
      console.info('[terminal] websocket close', {
        sandboxId,
        containerId: selectedContainerId,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      });
      if (!opened || failed) {
        setStatusText(t('terminal.failed'));
        writeStatusLine(t('terminal.backendUnavailable'), '33');
      } else {
        setStatusText(t('terminal.disconnected'));
        writeStatusLine(t('terminal.closed'), '33');
      }
    };

    term.onData((data) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(data);
      }
    });

    term.onResize((size) => {
      if (ws.readyState === WebSocket.OPEN) {
        const last = lastSentSizeRef.current;
        if (last?.rows === size.rows && last?.cols === size.cols) return;
        lastSentSizeRef.current = { rows: size.rows, cols: size.cols };
        console.info('[terminal] send resize', {
          sandboxId,
          containerId: selectedContainerId,
          rows: size.rows,
          cols: size.cols,
        });
        ws.send(JSON.stringify({ type: 'resize', rows: size.rows, cols: size.cols }));
      }
    });

    const scheduleFit = () => {
      if (isResizingPanelRef.current) return;
      if (resizeFrameRef.current !== null) return;
      resizeFrameRef.current = window.requestAnimationFrame(() => {
        resizeFrameRef.current = null;
        fitAddon.fit();
      });
    };

    const handleKeyDown = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target || !terminalElement.contains(target)) return;
      if (e.ctrlKey || e.metaKey) {
        switch (e.key.toLowerCase()) {
          case 'c':
            if (term.getSelection()) {
              e.preventDefault();
              handleCopy();
            }
            break;
          case 'v':
            e.preventDefault();
            handlePaste();
            break;
        }
      }
    };

    const handleMouseDown = () => {
      term.focus();
    };

    const observer = new ResizeObserver(() => {
      scheduleFit();
    });
    observer.observe(terminalElement);
    window.addEventListener('keydown', handleKeyDown);
    terminalElement.addEventListener('mousedown', handleMouseDown);
    const pingTimer = window.setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'ping' }));
      }
    }, 30_000);

    return () => {
      disposed = true;
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      ws.close();
      wsRef.current = null;
      if (resizeFrameRef.current !== null) {
        window.cancelAnimationFrame(resizeFrameRef.current);
        resizeFrameRef.current = null;
      }
      observer.disconnect();
      term.dispose();
      termRef.current = null;
      fitAddonRef.current = null;
      window.removeEventListener('keydown', handleKeyDown);
      terminalElement.removeEventListener('mousedown', handleMouseDown);
      window.clearInterval(pingTimer);
    };
  }, [
    credential,
    credentialParam,
    canConnect,
    sandboxId,
    handleCopy,
    handlePaste,
    connectionKey,
    selectedContainerId,
    t,
    writeStatusLine,
  ]);

  useEffect(() => {
    if (isFullscreen) return;
    setPanelRect((current) => clampPanelRect(current));
  }, [isFullscreen]);

  useEffect(() => {
    if (!termRef.current) return;
    termRef.current.options.fontSize = fontSize;
    window.requestAnimationFrame(() => fitAddonRef.current?.fit());
  }, [fontSize]);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      fitAddonRef.current?.fit();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [isFullscreen]);

  useEffect(() => {
    if (isFullscreen) return;
    setPanelRect((current) => clampPanelRect(current));
    const onWindowResize = () => {
      setPanelRect((current) => clampPanelRect(current));
    };
    window.addEventListener('resize', onWindowResize);
    return () => window.removeEventListener('resize', onWindowResize);
  }, [isFullscreen]);

  useEffect(() => {
    if (!isFullscreen) {
      lastWindowedRectRef.current = clampPanelRect(panelRect);
    }
  }, [isFullscreen, panelRect]);

  const toggleFullscreen = () => {
    setIsFullscreen((current) => {
      if (current) {
        setPanelRect((rect) => {
          const next = clampPanelRect(rect);
          lastWindowedRectRef.current = next;
          return next;
        });
        return false;
      }
      const next = clampPanelRect(lastWindowedRectRef.current ?? createInitialPanelRect());
      setPanelRect(next);
      return true;
    });
  };

  const reconnect = () => {
    setConnectionKey((key) => key + 1);
  };

  const updateFontSize = (value: string) => {
    const next = Number(value);
    if (!Number.isFinite(next)) return;
    setFontSize(Math.min(24, Math.max(10, next)));
  };

  const showContainerPicker = !selectedContainerId && !defaultContainerId && containers.length > 0;
  const containerLabel = selectedContainer
    ? `${selectedContainer.name || selectedContainer.containerID}`
    : selectedContainerId || '';
  const fullscreenRect = typeof window === 'undefined'
    ? createInitialPanelRect()
    : {
        width: window.innerWidth,
        height: window.innerHeight,
        left: 0,
        top: 0,
      };

  const panelBody = (
    <div className="flex h-full w-full flex-col overflow-hidden bg-[#0d1117]">
      <div className={`flex items-center justify-between border-b border-gray-700 bg-gray-900 px-4 py-2 ${isFullscreen ? 'terminal-drag-handle' : 'terminal-drag-handle cursor-move select-none'}`}>
        <div className="terminal-drag-handle flex min-w-0 items-center gap-3 cursor-move select-none">
          <span className="text-sm font-medium text-gray-300">{t('terminal.title')}</span>
          <span className="truncate text-xs text-gray-500">- {sandboxId}</span>
          <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-xs text-gray-500">{statusText}</span>
          {containerLabel && (
            <span className="truncate text-xs text-gray-500">
              · {t('terminal.containerLabel', { container: containerLabel })}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {canReconnect && (
            <Button size="icon" variant="ghost" onClick={reconnect} className="text-gray-400 hover:text-white" title={t('terminal.reconnect')} aria-label={t('terminal.reconnect')}>
              <RefreshCw size={14} />
            </Button>
          )}
          <label className="flex items-center gap-1 text-xs text-gray-500">
            <span>{t('terminal.fontSize')}</span>
            <Input
              aria-label={t('terminal.fontSize')}
              type="number"
              min={10}
              max={24}
              value={fontSize}
              onChange={(event) => updateFontSize(event.target.value)}
              className="h-7 w-14 border-gray-700 bg-gray-950 px-2 py-1 text-xs text-gray-300"
            />
          </label>
          <Button size="icon" variant="ghost" onClick={handleCopy} className="text-gray-400 hover:text-white" title={t('terminal.copy')} aria-label={t('terminal.copy')}>
            {copied ? <Check size={14} /> : <Copy size={14} />}
          </Button>
          <Button size="icon" variant="ghost" onClick={toggleFullscreen} className="text-gray-400 hover:text-white" title={isFullscreen ? t('terminal.minimize') : t('terminal.fullscreen')} aria-label={isFullscreen ? t('terminal.minimize') : t('terminal.fullscreen')}>
            {isFullscreen ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
          </Button>
          <Button size="icon" variant="ghost" onClick={onClose} className="text-gray-400 hover:text-white hover:bg-red-900/30" title={t('terminal.close')} aria-label={t('terminal.close')}>
            <X size={14} />
          </Button>
        </div>
      </div>
      {showContainerPicker ? (
        <div className="min-h-0 flex-1 overflow-auto p-4">
          {sandboxQuery.isLoading ? (
            <div className="text-sm text-gray-400">{t('terminal.loadingSandbox')}</div>
          ) : containers.length === 0 ? (
            <div className="text-sm text-red-300">{t('terminal.noContainers')}</div>
          ) : (
            <>
              <div className="mb-4 text-sm text-gray-300">
                {containers.length > 1 ? t('terminal.selectContainer') : t('terminal.preparingContainer')}
              </div>
              {containers.length > 1 ? (
                <div className="grid gap-2">
                  {containers.map((container) => {
                    const isPrimary = container.primary || container.containerID === sandboxId;
                    const isRunning = container.status.toLowerCase() === 'running';
                    return (
                      <button
                        key={container.containerID}
                        type="button"
                        onClick={() => setSelectedContainerId(container.containerID)}
                        disabled={!isRunning}
                        className="flex items-center justify-between gap-3 rounded-md border border-gray-700 bg-gray-950 px-3 py-3 text-left text-sm text-gray-200 transition hover:border-blue-500 hover:bg-gray-900 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        <div className="min-w-0">
                          <div className="flex min-w-0 items-center gap-2">
                            <span className="truncate font-medium">{container.name || container.containerID}</span>
                            {isPrimary && (
                              <span className="rounded border border-blue-500/40 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-blue-300">
                                {t('terminal.primaryContainer')}
                              </span>
                            )}
                            {!isRunning && (
                              <span className="rounded border border-red-500/40 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-red-300">
                                {container.status}
                              </span>
                            )}
                          </div>
                          <div className="mt-1 truncate text-xs text-gray-500">
                            {container.containerID}
                            {container.image ? ` · ${container.image}` : ''}
                          </div>
                        </div>
                        <span className="text-xs text-gray-500">{container.status}</span>
                      </button>
                    );
                  })}
                </div>
              ) : (
                <div className="text-sm text-gray-400">{t('terminal.selectContainer')}</div>
              )}
            </>
          )}
        </div>
      ) : (
        <div className="min-h-0 flex-1" ref={terminalRef} />
      )}
    </div>
  );

  return createPortal(
    <Rnd
      bounds="window"
      className={`z-[1000] ${isFullscreen ? 'fixed inset-0' : ''}`}
      size={isFullscreen
        ? { width: fullscreenRect.width, height: fullscreenRect.height }
        : { width: panelRect.width, height: panelRect.height }}
      position={isFullscreen
        ? { x: fullscreenRect.left, y: fullscreenRect.top }
        : { x: panelRect.left, y: panelRect.top }}
      minWidth={isFullscreen ? fullscreenRect.width : PANEL_MIN_WIDTH}
      minHeight={isFullscreen ? fullscreenRect.height : PANEL_MIN_HEIGHT}
      dragHandleClassName="terminal-drag-handle"
      disableDragging={isFullscreen}
      enableResizing={isFullscreen ? false : {
        top: true,
        right: true,
        bottom: true,
        left: true,
        topRight: true,
        topLeft: true,
        bottomRight: true,
        bottomLeft: true,
      }}
      onResizeStart={() => {
        if (isFullscreen) return;
        isResizingPanelRef.current = true;
        console.info('[terminal] resize start', {
          sandboxId,
          containerId: selectedContainerId,
        });
      }}
      onDragStop={(_e, data) => {
        if (isFullscreen) return;
        setPanelRect((current) => clampPanelRect({
          width: current.width,
          height: current.height,
          left: data.x,
          top: data.y,
        }));
      }}
      onResizeStop={(_e, _dir, ref, _delta, position) => {
        if (isFullscreen) return;
        const next = clampPanelRect({
          width: ref.offsetWidth,
          height: ref.offsetHeight,
          left: position.x,
          top: position.y,
        });
        isResizingPanelRef.current = false;
        console.info('[terminal] resize stop', {
          sandboxId,
          containerId: selectedContainerId,
          width: next.width,
          height: next.height,
          left: next.left,
          top: next.top,
        });
        setPanelRect(next);
        window.requestAnimationFrame(() => {
          fitAddonRef.current?.fit();
        });
      }}
      onResize={() => {
        if (isFullscreen) return;
        if (isResizingPanelRef.current) return;
        if (resizeFrameRef.current !== null) return;
        resizeFrameRef.current = window.requestAnimationFrame(() => {
          resizeFrameRef.current = null;
          fitAddonRef.current?.fit();
        });
      }}
    >
      <div className="h-full w-full rounded-lg shadow-2xl bg-[#0d1117]">
        {panelBody}
      </div>
    </Rnd>,
    document.body,
  );
}
