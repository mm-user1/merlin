"""Installed Chromium CDP harness; owned external profiles, trusted user gestures."""
import base64
import json
from pathlib import Path
import subprocess
import time
import urllib.request

import websocket


class Browser:
    def __init__(self, executable, profile, width=1440, height=1080):
        self.executable, self.profile = str(executable), Path(profile)
        self.width, self.height = width, height

    def __enter__(self):
        self.profile.mkdir(parents=True, exist_ok=False)
        self.log = (self.profile / 'browser.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([self.executable, '--headless=new', '--no-first-run',
            '--no-default-browser-check', '--disable-extensions', '--disable-background-networking',
            '--disable-component-update', '--disable-sync', '--remote-debugging-port=0',
            '--user-data-dir='+str(self.profile), 'about:blank'], stdout=self.log, stderr=self.log,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        self.seq, self.events, self.session, self.gestures = 0, [], None, []
        portfile = self.profile / 'DevToolsActivePort'
        for _ in range(100):
            if portfile.exists():
                port = int(portfile.read_text().splitlines()[0])
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/json/version', timeout=1) as response:
                        version = json.load(response)
                    break
                except OSError:
                    pass
            time.sleep(.1)
        else:
            self.process.terminate()
            raise RuntimeError('Owned installed browser did not expose CDP')
        self.version = version['Browser']
        self.ws = websocket.create_connection(version['webSocketDebuggerUrl'], timeout=30, suppress_origin=True)
        target = self.call('Target.createTarget', {'url':'about:blank'})['targetId']
        self.session = self.call('Target.attachToTarget', {'targetId':target, 'flatten':True})['sessionId']
        for domain in ('Page', 'Runtime', 'Network', 'Performance'):
            self.call(domain+'.enable')
        self.size(self.width, self.height)
        return self

    def call(self, method, params=None, *, browser=False):
        self.seq += 1
        message = dict(id=self.seq, method=method, params=params or {})
        if self.session and not browser:
            message['sessionId'] = self.session
        self.ws.send(json.dumps(message))
        while True:
            reply = json.loads(self.ws.recv())
            if reply.get('id') == self.seq:
                if 'error' in reply:
                    raise AssertionError(reply['error'])
                return reply.get('result', {})
            self.events.append(reply)

    def evaluate(self, expression):
        result = self.call('Runtime.evaluate', dict(expression=expression, returnByValue=True, awaitPromise=True))
        if 'exceptionDetails' in result:
            raise AssertionError(result['exceptionDetails'])
        return result['result'].get('value')

    def wait(self, expression='typeof reportDebug===\'function\' && (reportDebug().ready || reportDebug().error)', timeout=30):
        start = time.perf_counter()
        while time.perf_counter()-start < timeout:
            if self.evaluate(expression):
                return
            time.sleep(.02)
        raise AssertionError('Browser wait timed out: '+expression)

    def open(self, path):
        start = time.perf_counter()
        self.call('Page.navigate', {'url':Path(path).resolve().as_uri()})
        self.wait()
        assert not self.evaluate('reportDebug().error'), self.evaluate('reportDebug()')
        self.call('Runtime.evaluate', {'expression':'new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))', 'awaitPromise':True})
        return time.perf_counter()-start

    def size(self, width, height):
        self.call('Emulation.setDeviceMetricsOverride', dict(width=width, height=height, deviceScaleFactor=1, mobile=False))

    def mouse(self, kind, x, y, **options):
        self.call('Input.dispatchMouseEvent', dict(type=kind, x=x, y=y, **options))
        self.gestures.append(dict(method='Input.dispatchMouseEvent', type=kind, **options))

    def click(self, selector, modifiers=0, count=1):
        encoded = json.dumps(selector)
        # Scroll an off-screen virtual row into the rendered window. Only input
        # delivery below activates it; DOM setup does not synthesize a click.
        self.evaluate(f'''(async()=>{{if(document.querySelector({encoded}))return;
            const match={encoded}.match(/data-(?:inspect|select|account)="([^"]+)"/);
            if(!match)return;const index=reportDebug().rows.indexOf(match[1]);
            if(index>=0){{document.querySelector('.table-scroll').scrollTop=index*34;
                await new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));}}
        }})()''')
        point = self.evaluate(f'(()=>{{const e=document.querySelector({encoded});if(!e)throw Error("Missing target");e.scrollIntoView({{block:"center",inline:"nearest"}});const r=e.getBoundingClientRect();return {{x:r.x+r.width/2,y:r.y+r.height/2}};}})()')
        self.mouse('mouseMoved', **point, modifiers=modifiers)
        self.mouse('mousePressed', **point, button='left', clickCount=count, modifiers=modifiers)
        self.mouse('mouseReleased', **point, button='left', clickCount=count, modifiers=modifiers)

    def key(self, key, code=None, modifiers=0):
        params = dict(key=key, code=code or key, modifiers=modifiers)
        # Chromium's native defaults (scrolling/Space/selects) need the virtual
        # key code as well as the DOM key/code used by application listeners.
        virtual = {'ArrowLeft':37, 'ArrowUp':38, 'ArrowRight':39, 'ArrowDown':40,
                   'Home':36, 'End':35, 'Tab':9, 'Escape':27, 'Enter':13,
                   'Space':32, 'Equal':187, 'Minus':189}.get(params['code'])
        if params['code'].startswith('Key') and len(params['code']) == 4:
            virtual = ord(params['code'][-1])
        if params['code'].startswith('Digit') and len(params['code']) == 6:
            virtual = ord(params['code'][-1])
        if virtual is not None:
            params['windowsVirtualKeyCode'] = virtual
        for kind in ('keyDown', 'keyUp'):
            self.call('Input.dispatchKeyEvent', dict(type=kind, **params))
        self.gestures.append(dict(method='Input.dispatchKeyEvent', **params))

    def screenshot(self, path):
        Path(path).write_bytes(base64.b64decode(self.call('Page.captureScreenshot', {'format':'png'})['data']))

    def __exit__(self, *_):
        try:
            self.call('Browser.close')
        finally:
            self.ws.close()
            self.log.close()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=10)
