var __create = Object.create;
var __getProtoOf = Object.getPrototypeOf;
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
function __accessProp(key) {
  return this[key];
}
var __toESMCache_node;
var __toESMCache_esm;
var __toESM = (mod, isNodeMode, target) => {
  var canCache = mod != null && typeof mod === "object";
  if (canCache) {
    var cache = isNodeMode ? __toESMCache_node ??= new WeakMap : __toESMCache_esm ??= new WeakMap;
    var cached = cache.get(mod);
    if (cached)
      return cached;
  }
  target = mod != null ? __create(__getProtoOf(mod)) : {};
  const to = isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target;
  for (let key of __getOwnPropNames(mod))
    if (!__hasOwnProp.call(to, key))
      __defProp(to, key, {
        get: __accessProp.bind(mod, key),
        enumerable: true
      });
  if (canCache)
    cache.set(mod, to);
  return to;
};
var __commonJS = (cb, mod) => () => (mod || cb((mod = { exports: {} }).exports, mod), mod.exports);
var __returnValue = (v) => v;
function __exportSetter(name, newValue) {
  this[name] = __returnValue.bind(null, newValue);
}
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, {
      get: all[name],
      enumerable: true,
      configurable: true,
      set: __exportSetter.bind(all, name)
    });
};
var __esm = (fn, res) => () => (fn && (res = fn(fn = 0)), res);

// webapp/node_modules/react/cjs/react.production.min.js
var exports_react_production_min = {};
__export(exports_react_production_min, {
  version: () => $version,
  useTransition: () => $useTransition,
  useSyncExternalStore: () => $useSyncExternalStore,
  useState: () => $useState,
  useRef: () => $useRef,
  useReducer: () => $useReducer,
  useMemo: () => $useMemo,
  useLayoutEffect: () => $useLayoutEffect,
  useInsertionEffect: () => $useInsertionEffect,
  useImperativeHandle: () => $useImperativeHandle,
  useId: () => $useId,
  useEffect: () => $useEffect,
  useDeferredValue: () => $useDeferredValue,
  useDebugValue: () => $useDebugValue,
  useContext: () => $useContext,
  useCallback: () => $useCallback,
  unstable_act: () => $unstable_act,
  startTransition: () => $startTransition,
  memo: () => $memo,
  lazy: () => $lazy,
  isValidElement: () => $isValidElement,
  forwardRef: () => $forwardRef,
  createRef: () => $createRef,
  createFactory: () => $createFactory,
  createElement: () => $createElement,
  createContext: () => $createContext,
  cloneElement: () => $cloneElement,
  act: () => $act,
  __SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED: () => $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED,
  Suspense: () => $Suspense,
  StrictMode: () => $StrictMode,
  PureComponent: () => $PureComponent,
  Profiler: () => $Profiler,
  Fragment: () => $Fragment,
  Component: () => $Component,
  Children: () => $Children
});
function A(a) {
  if (a === null || typeof a !== "object")
    return null;
  a = z && a[z] || a["@@iterator"];
  return typeof a === "function" ? a : null;
}
function E(a, b, e) {
  this.props = a;
  this.context = b;
  this.refs = D;
  this.updater = e || B;
}
function F() {}
function G(a, b, e) {
  this.props = a;
  this.context = b;
  this.refs = D;
  this.updater = e || B;
}
function M(a, b, e) {
  var d, c = {}, k = null, h = null;
  if (b != null)
    for (d in b.ref !== undefined && (h = b.ref), b.key !== undefined && (k = "" + b.key), b)
      J.call(b, d) && !L.hasOwnProperty(d) && (c[d] = b[d]);
  var g = arguments.length - 2;
  if (g === 1)
    c.children = e;
  else if (1 < g) {
    for (var f = Array(g), m = 0;m < g; m++)
      f[m] = arguments[m + 2];
    c.children = f;
  }
  if (a && a.defaultProps)
    for (d in g = a.defaultProps, g)
      c[d] === undefined && (c[d] = g[d]);
  return { $$typeof: l, type: a, key: k, ref: h, props: c, _owner: K.current };
}
function N(a, b) {
  return { $$typeof: l, type: a.type, key: b, ref: a.ref, props: a.props, _owner: a._owner };
}
function O(a) {
  return typeof a === "object" && a !== null && a.$$typeof === l;
}
function escape(a) {
  var b = { "=": "=0", ":": "=2" };
  return "$" + a.replace(/[=:]/g, function(a2) {
    return b[a2];
  });
}
function Q(a, b) {
  return typeof a === "object" && a !== null && a.key != null ? escape("" + a.key) : b.toString(36);
}
function R(a, b, e, d, c) {
  var k = typeof a;
  if (k === "undefined" || k === "boolean")
    a = null;
  var h = false;
  if (a === null)
    h = true;
  else
    switch (k) {
      case "string":
      case "number":
        h = true;
        break;
      case "object":
        switch (a.$$typeof) {
          case l:
          case n:
            h = true;
        }
    }
  if (h)
    return h = a, c = c(h), a = d === "" ? "." + Q(h, 0) : d, I(c) ? (e = "", a != null && (e = a.replace(P, "$&/") + "/"), R(c, b, e, "", function(a2) {
      return a2;
    })) : c != null && (O(c) && (c = N(c, e + (!c.key || h && h.key === c.key ? "" : ("" + c.key).replace(P, "$&/") + "/") + a)), b.push(c)), 1;
  h = 0;
  d = d === "" ? "." : d + ":";
  if (I(a))
    for (var g = 0;g < a.length; g++) {
      k = a[g];
      var f = d + Q(k, g);
      h += R(k, b, e, f, c);
    }
  else if (f = A(a), typeof f === "function")
    for (a = f.call(a), g = 0;!(k = a.next()).done; )
      k = k.value, f = d + Q(k, g++), h += R(k, b, e, f, c);
  else if (k === "object")
    throw b = String(a), Error("Objects are not valid as a React child (found: " + (b === "[object Object]" ? "object with keys {" + Object.keys(a).join(", ") + "}" : b) + "). If you meant to render a collection of children, use an array instead.");
  return h;
}
function S(a, b, e) {
  if (a == null)
    return a;
  var d = [], c = 0;
  R(a, d, "", "", function(a2) {
    return b.call(e, a2, c++);
  });
  return d;
}
function T(a) {
  if (a._status === -1) {
    var b = a._result;
    b = b();
    b.then(function(b2) {
      if (a._status === 0 || a._status === -1)
        a._status = 1, a._result = b2;
    }, function(b2) {
      if (a._status === 0 || a._status === -1)
        a._status = 2, a._result = b2;
    });
    a._status === -1 && (a._status = 0, a._result = b);
  }
  if (a._status === 1)
    return a._result.default;
  throw a._result;
}
function X() {
  throw Error("act(...) is not supported in production builds of React.");
}
var l, n, p, q, r, t, u, v, w, x, y, z, B, C, D, H, I, J, K, L, P, U, V, W, $Children, $Component, $Fragment, $Profiler, $PureComponent, $StrictMode, $Suspense, $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED, $act, $cloneElement = function(a, b, e) {
  if (a === null || a === undefined)
    throw Error("React.cloneElement(...): The argument must be a React element, but you passed " + a + ".");
  var d = C({}, a.props), c = a.key, k = a.ref, h = a._owner;
  if (b != null) {
    b.ref !== undefined && (k = b.ref, h = K.current);
    b.key !== undefined && (c = "" + b.key);
    if (a.type && a.type.defaultProps)
      var g = a.type.defaultProps;
    for (f in b)
      J.call(b, f) && !L.hasOwnProperty(f) && (d[f] = b[f] === undefined && g !== undefined ? g[f] : b[f]);
  }
  var f = arguments.length - 2;
  if (f === 1)
    d.children = e;
  else if (1 < f) {
    g = Array(f);
    for (var m = 0;m < f; m++)
      g[m] = arguments[m + 2];
    d.children = g;
  }
  return { $$typeof: l, type: a.type, key: c, ref: k, props: d, _owner: h };
}, $createContext = function(a) {
  a = { $$typeof: u, _currentValue: a, _currentValue2: a, _threadCount: 0, Provider: null, Consumer: null, _defaultValue: null, _globalName: null };
  a.Provider = { $$typeof: t, _context: a };
  return a.Consumer = a;
}, $createElement, $createFactory = function(a) {
  var b = M.bind(null, a);
  b.type = a;
  return b;
}, $createRef = function() {
  return { current: null };
}, $forwardRef = function(a) {
  return { $$typeof: v, render: a };
}, $isValidElement, $lazy = function(a) {
  return { $$typeof: y, _payload: { _status: -1, _result: a }, _init: T };
}, $memo = function(a, b) {
  return { $$typeof: x, type: a, compare: b === undefined ? null : b };
}, $startTransition = function(a) {
  var b = V.transition;
  V.transition = {};
  try {
    a();
  } finally {
    V.transition = b;
  }
}, $unstable_act, $useCallback = function(a, b) {
  return U.current.useCallback(a, b);
}, $useContext = function(a) {
  return U.current.useContext(a);
}, $useDebugValue = function() {}, $useDeferredValue = function(a) {
  return U.current.useDeferredValue(a);
}, $useEffect = function(a, b) {
  return U.current.useEffect(a, b);
}, $useId = function() {
  return U.current.useId();
}, $useImperativeHandle = function(a, b, e) {
  return U.current.useImperativeHandle(a, b, e);
}, $useInsertionEffect = function(a, b) {
  return U.current.useInsertionEffect(a, b);
}, $useLayoutEffect = function(a, b) {
  return U.current.useLayoutEffect(a, b);
}, $useMemo = function(a, b) {
  return U.current.useMemo(a, b);
}, $useReducer = function(a, b, e) {
  return U.current.useReducer(a, b, e);
}, $useRef = function(a) {
  return U.current.useRef(a);
}, $useState = function(a) {
  return U.current.useState(a);
}, $useSyncExternalStore = function(a, b, e) {
  return U.current.useSyncExternalStore(a, b, e);
}, $useTransition = function() {
  return U.current.useTransition();
}, $version = "18.3.1";
var init_react_production_min = __esm(() => {
  l = Symbol.for("react.element");
  n = Symbol.for("react.portal");
  p = Symbol.for("react.fragment");
  q = Symbol.for("react.strict_mode");
  r = Symbol.for("react.profiler");
  t = Symbol.for("react.provider");
  u = Symbol.for("react.context");
  v = Symbol.for("react.forward_ref");
  w = Symbol.for("react.suspense");
  x = Symbol.for("react.memo");
  y = Symbol.for("react.lazy");
  z = Symbol.iterator;
  B = { isMounted: function() {
    return false;
  }, enqueueForceUpdate: function() {}, enqueueReplaceState: function() {}, enqueueSetState: function() {} };
  C = Object.assign;
  D = {};
  E.prototype.isReactComponent = {};
  E.prototype.setState = function(a, b) {
    if (typeof a !== "object" && typeof a !== "function" && a != null)
      throw Error("setState(...): takes an object of state variables to update or a function which returns an object of state variables.");
    this.updater.enqueueSetState(this, a, b, "setState");
  };
  E.prototype.forceUpdate = function(a) {
    this.updater.enqueueForceUpdate(this, a, "forceUpdate");
  };
  F.prototype = E.prototype;
  H = G.prototype = new F;
  H.constructor = G;
  C(H, E.prototype);
  H.isPureReactComponent = true;
  I = Array.isArray;
  J = Object.prototype.hasOwnProperty;
  K = { current: null };
  L = { key: true, ref: true, __self: true, __source: true };
  P = /\/+/g;
  U = { current: null };
  V = { transition: null };
  W = { ReactCurrentDispatcher: U, ReactCurrentBatchConfig: V, ReactCurrentOwner: K };
  $Children = { map: S, forEach: function(a, b, e) {
    S(a, function() {
      b.apply(this, arguments);
    }, e);
  }, count: function(a) {
    var b = 0;
    S(a, function() {
      b++;
    });
    return b;
  }, toArray: function(a) {
    return S(a, function(a2) {
      return a2;
    }) || [];
  }, only: function(a) {
    if (!O(a))
      throw Error("React.Children.only expected to receive a single React element child.");
    return a;
  } };
  $Component = E;
  $Fragment = p;
  $Profiler = r;
  $PureComponent = G;
  $StrictMode = q;
  $Suspense = w;
  $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED = W;
  $act = X;
  $createElement = M;
  $isValidElement = O;
  $unstable_act = X;
});

// webapp/node_modules/react/index.js
var require_react = __commonJS((exports, module) => {
  init_react_production_min();
  if (true) {
    module.exports = exports_react_production_min;
  }
});

// webapp/node_modules/scheduler/cjs/scheduler.production.min.js
var require_scheduler_production_min = __commonJS((exports) => {
  function f(a, b) {
    var c = a.length;
    a.push(b);
    a:
      for (;0 < c; ) {
        var d = c - 1 >>> 1, e = a[d];
        if (0 < g(e, b))
          a[d] = b, a[c] = e, c = d;
        else
          break a;
      }
  }
  function h(a) {
    return a.length === 0 ? null : a[0];
  }
  function k(a) {
    if (a.length === 0)
      return null;
    var b = a[0], c = a.pop();
    if (c !== b) {
      a[0] = c;
      a:
        for (var d = 0, e = a.length, w2 = e >>> 1;d < w2; ) {
          var m = 2 * (d + 1) - 1, C2 = a[m], n2 = m + 1, x2 = a[n2];
          if (0 > g(C2, c))
            n2 < e && 0 > g(x2, C2) ? (a[d] = x2, a[n2] = c, d = n2) : (a[d] = C2, a[m] = c, d = m);
          else if (n2 < e && 0 > g(x2, c))
            a[d] = x2, a[n2] = c, d = n2;
          else
            break a;
        }
    }
    return b;
  }
  function g(a, b) {
    var c = a.sortIndex - b.sortIndex;
    return c !== 0 ? c : a.id - b.id;
  }
  if (typeof performance === "object" && typeof performance.now === "function") {
    l2 = performance;
    exports.unstable_now = function() {
      return l2.now();
    };
  } else {
    p2 = Date, q2 = p2.now();
    exports.unstable_now = function() {
      return p2.now() - q2;
    };
  }
  var l2;
  var p2;
  var q2;
  var r2 = [];
  var t2 = [];
  var u2 = 1;
  var v2 = null;
  var y2 = 3;
  var z2 = false;
  var A2 = false;
  var B2 = false;
  var D2 = typeof setTimeout === "function" ? setTimeout : null;
  var E2 = typeof clearTimeout === "function" ? clearTimeout : null;
  var F2 = typeof setImmediate !== "undefined" ? setImmediate : null;
  typeof navigator !== "undefined" && navigator.scheduling !== undefined && navigator.scheduling.isInputPending !== undefined && navigator.scheduling.isInputPending.bind(navigator.scheduling);
  function G2(a) {
    for (var b = h(t2);b !== null; ) {
      if (b.callback === null)
        k(t2);
      else if (b.startTime <= a)
        k(t2), b.sortIndex = b.expirationTime, f(r2, b);
      else
        break;
      b = h(t2);
    }
  }
  function H2(a) {
    B2 = false;
    G2(a);
    if (!A2)
      if (h(r2) !== null)
        A2 = true, I2(J2);
      else {
        var b = h(t2);
        b !== null && K2(H2, b.startTime - a);
      }
  }
  function J2(a, b) {
    A2 = false;
    B2 && (B2 = false, E2(L2), L2 = -1);
    z2 = true;
    var c = y2;
    try {
      G2(b);
      for (v2 = h(r2);v2 !== null && (!(v2.expirationTime > b) || a && !M2()); ) {
        var d = v2.callback;
        if (typeof d === "function") {
          v2.callback = null;
          y2 = v2.priorityLevel;
          var e = d(v2.expirationTime <= b);
          b = exports.unstable_now();
          typeof e === "function" ? v2.callback = e : v2 === h(r2) && k(r2);
          G2(b);
        } else
          k(r2);
        v2 = h(r2);
      }
      if (v2 !== null)
        var w2 = true;
      else {
        var m = h(t2);
        m !== null && K2(H2, m.startTime - b);
        w2 = false;
      }
      return w2;
    } finally {
      v2 = null, y2 = c, z2 = false;
    }
  }
  var N2 = false;
  var O2 = null;
  var L2 = -1;
  var P2 = 5;
  var Q2 = -1;
  function M2() {
    return exports.unstable_now() - Q2 < P2 ? false : true;
  }
  function R2() {
    if (O2 !== null) {
      var a = exports.unstable_now();
      Q2 = a;
      var b = true;
      try {
        b = O2(true, a);
      } finally {
        b ? S2() : (N2 = false, O2 = null);
      }
    } else
      N2 = false;
  }
  var S2;
  if (typeof F2 === "function")
    S2 = function() {
      F2(R2);
    };
  else if (typeof MessageChannel !== "undefined") {
    T2 = new MessageChannel, U2 = T2.port2;
    T2.port1.onmessage = R2;
    S2 = function() {
      U2.postMessage(null);
    };
  } else
    S2 = function() {
      D2(R2, 0);
    };
  var T2;
  var U2;
  function I2(a) {
    O2 = a;
    N2 || (N2 = true, S2());
  }
  function K2(a, b) {
    L2 = D2(function() {
      a(exports.unstable_now());
    }, b);
  }
  exports.unstable_IdlePriority = 5;
  exports.unstable_ImmediatePriority = 1;
  exports.unstable_LowPriority = 4;
  exports.unstable_NormalPriority = 3;
  exports.unstable_Profiling = null;
  exports.unstable_UserBlockingPriority = 2;
  exports.unstable_cancelCallback = function(a) {
    a.callback = null;
  };
  exports.unstable_continueExecution = function() {
    A2 || z2 || (A2 = true, I2(J2));
  };
  exports.unstable_forceFrameRate = function(a) {
    0 > a || 125 < a ? console.error("forceFrameRate takes a positive int between 0 and 125, forcing frame rates higher than 125 fps is not supported") : P2 = 0 < a ? Math.floor(1000 / a) : 5;
  };
  exports.unstable_getCurrentPriorityLevel = function() {
    return y2;
  };
  exports.unstable_getFirstCallbackNode = function() {
    return h(r2);
  };
  exports.unstable_next = function(a) {
    switch (y2) {
      case 1:
      case 2:
      case 3:
        var b = 3;
        break;
      default:
        b = y2;
    }
    var c = y2;
    y2 = b;
    try {
      return a();
    } finally {
      y2 = c;
    }
  };
  exports.unstable_pauseExecution = function() {};
  exports.unstable_requestPaint = function() {};
  exports.unstable_runWithPriority = function(a, b) {
    switch (a) {
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
        break;
      default:
        a = 3;
    }
    var c = y2;
    y2 = a;
    try {
      return b();
    } finally {
      y2 = c;
    }
  };
  exports.unstable_scheduleCallback = function(a, b, c) {
    var d = exports.unstable_now();
    typeof c === "object" && c !== null ? (c = c.delay, c = typeof c === "number" && 0 < c ? d + c : d) : c = d;
    switch (a) {
      case 1:
        var e = -1;
        break;
      case 2:
        e = 250;
        break;
      case 5:
        e = 1073741823;
        break;
      case 4:
        e = 1e4;
        break;
      default:
        e = 5000;
    }
    e = c + e;
    a = { id: u2++, callback: b, priorityLevel: a, startTime: c, expirationTime: e, sortIndex: -1 };
    c > d ? (a.sortIndex = c, f(t2, a), h(r2) === null && a === h(t2) && (B2 ? (E2(L2), L2 = -1) : B2 = true, K2(H2, c - d))) : (a.sortIndex = e, f(r2, a), A2 || z2 || (A2 = true, I2(J2)));
    return a;
  };
  exports.unstable_shouldYield = M2;
  exports.unstable_wrapCallback = function(a) {
    var b = y2;
    return function() {
      var c = y2;
      y2 = b;
      try {
        return a.apply(this, arguments);
      } finally {
        y2 = c;
      }
    };
  };
});

// webapp/node_modules/scheduler/index.js
var require_scheduler = __commonJS((exports, module) => {
  var scheduler_production_min = __toESM(require_scheduler_production_min());
  if (true) {
    module.exports = scheduler_production_min;
  }
});

// webapp/node_modules/react-dom/cjs/react-dom.production.min.js
var exports_react_dom_production_min = {};
__export(exports_react_dom_production_min, {
  version: () => $version2,
  unstable_renderSubtreeIntoContainer: () => $unstable_renderSubtreeIntoContainer,
  unstable_batchedUpdates: () => $unstable_batchedUpdates,
  unmountComponentAtNode: () => $unmountComponentAtNode,
  render: () => $render,
  hydrateRoot: () => $hydrateRoot,
  hydrate: () => $hydrate,
  flushSync: () => $flushSync,
  findDOMNode: () => $findDOMNode,
  createRoot: () => $createRoot,
  createPortal: () => $createPortal,
  __SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED: () => $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED2
});
function p2(a) {
  for (var b = "https://reactjs.org/docs/error-decoder.html?invariant=" + a, c = 1;c < arguments.length; c++)
    b += "&args[]=" + encodeURIComponent(arguments[c]);
  return "Minified React error #" + a + "; visit " + b + " for the full message or use the non-minified dev environment for full errors and additional helpful warnings.";
}
function fa(a, b) {
  ha(a, b);
  ha(a + "Capture", b);
}
function ha(a, b) {
  ea[a] = b;
  for (a = 0;a < b.length; a++)
    da.add(b[a]);
}
function oa(a) {
  if (ja.call(ma, a))
    return true;
  if (ja.call(la, a))
    return false;
  if (ka.test(a))
    return ma[a] = true;
  la[a] = true;
  return false;
}
function pa(a, b, c, d) {
  if (c !== null && c.type === 0)
    return false;
  switch (typeof b) {
    case "function":
    case "symbol":
      return true;
    case "boolean":
      if (d)
        return false;
      if (c !== null)
        return !c.acceptsBooleans;
      a = a.toLowerCase().slice(0, 5);
      return a !== "data-" && a !== "aria-";
    default:
      return false;
  }
}
function qa(a, b, c, d) {
  if (b === null || typeof b === "undefined" || pa(a, b, c, d))
    return true;
  if (d)
    return false;
  if (c !== null)
    switch (c.type) {
      case 3:
        return !b;
      case 4:
        return b === false;
      case 5:
        return isNaN(b);
      case 6:
        return isNaN(b) || 1 > b;
    }
  return false;
}
function v2(a, b, c, d, e, f, g) {
  this.acceptsBooleans = b === 2 || b === 3 || b === 4;
  this.attributeName = d;
  this.attributeNamespace = e;
  this.mustUseProperty = c;
  this.propertyName = a;
  this.type = b;
  this.sanitizeURL = f;
  this.removeEmptyString = g;
}
function sa(a) {
  return a[1].toUpperCase();
}
function ta(a, b, c, d) {
  var e = z2.hasOwnProperty(b) ? z2[b] : null;
  if (e !== null ? e.type !== 0 : d || !(2 < b.length) || b[0] !== "o" && b[0] !== "O" || b[1] !== "n" && b[1] !== "N")
    qa(b, c, e, d) && (c = null), d || e === null ? oa(b) && (c === null ? a.removeAttribute(b) : a.setAttribute(b, "" + c)) : e.mustUseProperty ? a[e.propertyName] = c === null ? e.type === 3 ? false : "" : c : (b = e.attributeName, d = e.attributeNamespace, c === null ? a.removeAttribute(b) : (e = e.type, c = e === 3 || e === 4 && c === true ? "" : "" + c, d ? a.setAttributeNS(d, b, c) : a.setAttribute(b, c)));
}
function Ka(a) {
  if (a === null || typeof a !== "object")
    return null;
  a = Ja && a[Ja] || a["@@iterator"];
  return typeof a === "function" ? a : null;
}
function Ma(a) {
  if (La === undefined)
    try {
      throw Error();
    } catch (c) {
      var b = c.stack.trim().match(/\n( *(at )?)/);
      La = b && b[1] || "";
    }
  return `
` + La + a;
}
function Oa(a, b) {
  if (!a || Na)
    return "";
  Na = true;
  var c = Error.prepareStackTrace;
  Error.prepareStackTrace = undefined;
  try {
    if (b)
      if (b = function() {
        throw Error();
      }, Object.defineProperty(b.prototype, "props", { set: function() {
        throw Error();
      } }), typeof Reflect === "object" && Reflect.construct) {
        try {
          Reflect.construct(b, []);
        } catch (l2) {
          var d = l2;
        }
        Reflect.construct(a, [], b);
      } else {
        try {
          b.call();
        } catch (l2) {
          d = l2;
        }
        a.call(b.prototype);
      }
    else {
      try {
        throw Error();
      } catch (l2) {
        d = l2;
      }
      a();
    }
  } catch (l2) {
    if (l2 && d && typeof l2.stack === "string") {
      for (var e = l2.stack.split(`
`), f = d.stack.split(`
`), g = e.length - 1, h = f.length - 1;1 <= g && 0 <= h && e[g] !== f[h]; )
        h--;
      for (;1 <= g && 0 <= h; g--, h--)
        if (e[g] !== f[h]) {
          if (g !== 1 || h !== 1) {
            do
              if (g--, h--, 0 > h || e[g] !== f[h]) {
                var k = `
` + e[g].replace(" at new ", " at ");
                a.displayName && k.includes("<anonymous>") && (k = k.replace("<anonymous>", a.displayName));
                return k;
              }
            while (1 <= g && 0 <= h);
          }
          break;
        }
    }
  } finally {
    Na = false, Error.prepareStackTrace = c;
  }
  return (a = a ? a.displayName || a.name : "") ? Ma(a) : "";
}
function Pa(a) {
  switch (a.tag) {
    case 5:
      return Ma(a.type);
    case 16:
      return Ma("Lazy");
    case 13:
      return Ma("Suspense");
    case 19:
      return Ma("SuspenseList");
    case 0:
    case 2:
    case 15:
      return a = Oa(a.type, false), a;
    case 11:
      return a = Oa(a.type.render, false), a;
    case 1:
      return a = Oa(a.type, true), a;
    default:
      return "";
  }
}
function Qa(a) {
  if (a == null)
    return null;
  if (typeof a === "function")
    return a.displayName || a.name || null;
  if (typeof a === "string")
    return a;
  switch (a) {
    case ya:
      return "Fragment";
    case wa:
      return "Portal";
    case Aa:
      return "Profiler";
    case za:
      return "StrictMode";
    case Ea:
      return "Suspense";
    case Fa:
      return "SuspenseList";
  }
  if (typeof a === "object")
    switch (a.$$typeof) {
      case Ca:
        return (a.displayName || "Context") + ".Consumer";
      case Ba:
        return (a._context.displayName || "Context") + ".Provider";
      case Da:
        var b = a.render;
        a = a.displayName;
        a || (a = b.displayName || b.name || "", a = a !== "" ? "ForwardRef(" + a + ")" : "ForwardRef");
        return a;
      case Ga:
        return b = a.displayName || null, b !== null ? b : Qa(a.type) || "Memo";
      case Ha:
        b = a._payload;
        a = a._init;
        try {
          return Qa(a(b));
        } catch (c) {}
    }
  return null;
}
function Ra(a) {
  var b = a.type;
  switch (a.tag) {
    case 24:
      return "Cache";
    case 9:
      return (b.displayName || "Context") + ".Consumer";
    case 10:
      return (b._context.displayName || "Context") + ".Provider";
    case 18:
      return "DehydratedFragment";
    case 11:
      return a = b.render, a = a.displayName || a.name || "", b.displayName || (a !== "" ? "ForwardRef(" + a + ")" : "ForwardRef");
    case 7:
      return "Fragment";
    case 5:
      return b;
    case 4:
      return "Portal";
    case 3:
      return "Root";
    case 6:
      return "Text";
    case 16:
      return Qa(b);
    case 8:
      return b === za ? "StrictMode" : "Mode";
    case 22:
      return "Offscreen";
    case 12:
      return "Profiler";
    case 21:
      return "Scope";
    case 13:
      return "Suspense";
    case 19:
      return "SuspenseList";
    case 25:
      return "TracingMarker";
    case 1:
    case 0:
    case 17:
    case 2:
    case 14:
    case 15:
      if (typeof b === "function")
        return b.displayName || b.name || null;
      if (typeof b === "string")
        return b;
  }
  return null;
}
function Sa(a) {
  switch (typeof a) {
    case "boolean":
    case "number":
    case "string":
    case "undefined":
      return a;
    case "object":
      return a;
    default:
      return "";
  }
}
function Ta(a) {
  var b = a.type;
  return (a = a.nodeName) && a.toLowerCase() === "input" && (b === "checkbox" || b === "radio");
}
function Ua(a) {
  var b = Ta(a) ? "checked" : "value", c = Object.getOwnPropertyDescriptor(a.constructor.prototype, b), d = "" + a[b];
  if (!a.hasOwnProperty(b) && typeof c !== "undefined" && typeof c.get === "function" && typeof c.set === "function") {
    var { get: e, set: f } = c;
    Object.defineProperty(a, b, { configurable: true, get: function() {
      return e.call(this);
    }, set: function(a2) {
      d = "" + a2;
      f.call(this, a2);
    } });
    Object.defineProperty(a, b, { enumerable: c.enumerable });
    return { getValue: function() {
      return d;
    }, setValue: function(a2) {
      d = "" + a2;
    }, stopTracking: function() {
      a._valueTracker = null;
      delete a[b];
    } };
  }
}
function Va(a) {
  a._valueTracker || (a._valueTracker = Ua(a));
}
function Wa(a) {
  if (!a)
    return false;
  var b = a._valueTracker;
  if (!b)
    return true;
  var c = b.getValue();
  var d = "";
  a && (d = Ta(a) ? a.checked ? "true" : "false" : a.value);
  a = d;
  return a !== c ? (b.setValue(a), true) : false;
}
function Xa(a) {
  a = a || (typeof document !== "undefined" ? document : undefined);
  if (typeof a === "undefined")
    return null;
  try {
    return a.activeElement || a.body;
  } catch (b) {
    return a.body;
  }
}
function Ya(a, b) {
  var c = b.checked;
  return A2({}, b, { defaultChecked: undefined, defaultValue: undefined, value: undefined, checked: c != null ? c : a._wrapperState.initialChecked });
}
function Za(a, b) {
  var c = b.defaultValue == null ? "" : b.defaultValue, d = b.checked != null ? b.checked : b.defaultChecked;
  c = Sa(b.value != null ? b.value : c);
  a._wrapperState = { initialChecked: d, initialValue: c, controlled: b.type === "checkbox" || b.type === "radio" ? b.checked != null : b.value != null };
}
function ab(a, b) {
  b = b.checked;
  b != null && ta(a, "checked", b, false);
}
function bb(a, b) {
  ab(a, b);
  var c = Sa(b.value), d = b.type;
  if (c != null)
    if (d === "number") {
      if (c === 0 && a.value === "" || a.value != c)
        a.value = "" + c;
    } else
      a.value !== "" + c && (a.value = "" + c);
  else if (d === "submit" || d === "reset") {
    a.removeAttribute("value");
    return;
  }
  b.hasOwnProperty("value") ? cb(a, b.type, c) : b.hasOwnProperty("defaultValue") && cb(a, b.type, Sa(b.defaultValue));
  b.checked == null && b.defaultChecked != null && (a.defaultChecked = !!b.defaultChecked);
}
function db(a, b, c) {
  if (b.hasOwnProperty("value") || b.hasOwnProperty("defaultValue")) {
    var d = b.type;
    if (!(d !== "submit" && d !== "reset" || b.value !== undefined && b.value !== null))
      return;
    b = "" + a._wrapperState.initialValue;
    c || b === a.value || (a.value = b);
    a.defaultValue = b;
  }
  c = a.name;
  c !== "" && (a.name = "");
  a.defaultChecked = !!a._wrapperState.initialChecked;
  c !== "" && (a.name = c);
}
function cb(a, b, c) {
  if (b !== "number" || Xa(a.ownerDocument) !== a)
    c == null ? a.defaultValue = "" + a._wrapperState.initialValue : a.defaultValue !== "" + c && (a.defaultValue = "" + c);
}
function fb(a, b, c, d) {
  a = a.options;
  if (b) {
    b = {};
    for (var e = 0;e < c.length; e++)
      b["$" + c[e]] = true;
    for (c = 0;c < a.length; c++)
      e = b.hasOwnProperty("$" + a[c].value), a[c].selected !== e && (a[c].selected = e), e && d && (a[c].defaultSelected = true);
  } else {
    c = "" + Sa(c);
    b = null;
    for (e = 0;e < a.length; e++) {
      if (a[e].value === c) {
        a[e].selected = true;
        d && (a[e].defaultSelected = true);
        return;
      }
      b !== null || a[e].disabled || (b = a[e]);
    }
    b !== null && (b.selected = true);
  }
}
function gb(a, b) {
  if (b.dangerouslySetInnerHTML != null)
    throw Error(p2(91));
  return A2({}, b, { value: undefined, defaultValue: undefined, children: "" + a._wrapperState.initialValue });
}
function hb(a, b) {
  var c = b.value;
  if (c == null) {
    c = b.children;
    b = b.defaultValue;
    if (c != null) {
      if (b != null)
        throw Error(p2(92));
      if (eb(c)) {
        if (1 < c.length)
          throw Error(p2(93));
        c = c[0];
      }
      b = c;
    }
    b == null && (b = "");
    c = b;
  }
  a._wrapperState = { initialValue: Sa(c) };
}
function ib(a, b) {
  var c = Sa(b.value), d = Sa(b.defaultValue);
  c != null && (c = "" + c, c !== a.value && (a.value = c), b.defaultValue == null && a.defaultValue !== c && (a.defaultValue = c));
  d != null && (a.defaultValue = "" + d);
}
function jb(a) {
  var b = a.textContent;
  b === a._wrapperState.initialValue && b !== "" && b !== null && (a.value = b);
}
function kb(a) {
  switch (a) {
    case "svg":
      return "http://www.w3.org/2000/svg";
    case "math":
      return "http://www.w3.org/1998/Math/MathML";
    default:
      return "http://www.w3.org/1999/xhtml";
  }
}
function lb(a, b) {
  return a == null || a === "http://www.w3.org/1999/xhtml" ? kb(b) : a === "http://www.w3.org/2000/svg" && b === "foreignObject" ? "http://www.w3.org/1999/xhtml" : a;
}
function ob(a, b) {
  if (b) {
    var c = a.firstChild;
    if (c && c === a.lastChild && c.nodeType === 3) {
      c.nodeValue = b;
      return;
    }
  }
  a.textContent = b;
}
function rb(a, b, c) {
  return b == null || typeof b === "boolean" || b === "" ? "" : c || typeof b !== "number" || b === 0 || pb.hasOwnProperty(a) && pb[a] ? ("" + b).trim() : b + "px";
}
function sb(a, b) {
  a = a.style;
  for (var c in b)
    if (b.hasOwnProperty(c)) {
      var d = c.indexOf("--") === 0, e = rb(c, b[c], d);
      c === "float" && (c = "cssFloat");
      d ? a.setProperty(c, e) : a[c] = e;
    }
}
function ub(a, b) {
  if (b) {
    if (tb[a] && (b.children != null || b.dangerouslySetInnerHTML != null))
      throw Error(p2(137, a));
    if (b.dangerouslySetInnerHTML != null) {
      if (b.children != null)
        throw Error(p2(60));
      if (typeof b.dangerouslySetInnerHTML !== "object" || !("__html" in b.dangerouslySetInnerHTML))
        throw Error(p2(61));
    }
    if (b.style != null && typeof b.style !== "object")
      throw Error(p2(62));
  }
}
function vb(a, b) {
  if (a.indexOf("-") === -1)
    return typeof b.is === "string";
  switch (a) {
    case "annotation-xml":
    case "color-profile":
    case "font-face":
    case "font-face-src":
    case "font-face-uri":
    case "font-face-format":
    case "font-face-name":
    case "missing-glyph":
      return false;
    default:
      return true;
  }
}
function xb(a) {
  a = a.target || a.srcElement || window;
  a.correspondingUseElement && (a = a.correspondingUseElement);
  return a.nodeType === 3 ? a.parentNode : a;
}
function Bb(a) {
  if (a = Cb(a)) {
    if (typeof yb !== "function")
      throw Error(p2(280));
    var b = a.stateNode;
    b && (b = Db(b), yb(a.stateNode, a.type, b));
  }
}
function Eb(a) {
  zb ? Ab ? Ab.push(a) : Ab = [a] : zb = a;
}
function Fb() {
  if (zb) {
    var a = zb, b = Ab;
    Ab = zb = null;
    Bb(a);
    if (b)
      for (a = 0;a < b.length; a++)
        Bb(b[a]);
  }
}
function Gb(a, b) {
  return a(b);
}
function Hb() {}
function Jb(a, b, c) {
  if (Ib)
    return a(b, c);
  Ib = true;
  try {
    return Gb(a, b, c);
  } finally {
    if (Ib = false, zb !== null || Ab !== null)
      Hb(), Fb();
  }
}
function Kb(a, b) {
  var c = a.stateNode;
  if (c === null)
    return null;
  var d = Db(c);
  if (d === null)
    return null;
  c = d[b];
  a:
    switch (b) {
      case "onClick":
      case "onClickCapture":
      case "onDoubleClick":
      case "onDoubleClickCapture":
      case "onMouseDown":
      case "onMouseDownCapture":
      case "onMouseMove":
      case "onMouseMoveCapture":
      case "onMouseUp":
      case "onMouseUpCapture":
      case "onMouseEnter":
        (d = !d.disabled) || (a = a.type, d = !(a === "button" || a === "input" || a === "select" || a === "textarea"));
        a = !d;
        break a;
      default:
        a = false;
    }
  if (a)
    return null;
  if (c && typeof c !== "function")
    throw Error(p2(231, b, typeof c));
  return c;
}
function Nb(a, b, c, d, e, f, g, h, k) {
  var l2 = Array.prototype.slice.call(arguments, 3);
  try {
    b.apply(c, l2);
  } catch (m) {
    this.onError(m);
  }
}
function Tb(a, b, c, d, e, f, g, h, k) {
  Ob = false;
  Pb = null;
  Nb.apply(Sb, arguments);
}
function Ub(a, b, c, d, e, f, g, h, k) {
  Tb.apply(this, arguments);
  if (Ob) {
    if (Ob) {
      var l2 = Pb;
      Ob = false;
      Pb = null;
    } else
      throw Error(p2(198));
    Qb || (Qb = true, Rb = l2);
  }
}
function Vb(a) {
  var b = a, c = a;
  if (a.alternate)
    for (;b.return; )
      b = b.return;
  else {
    a = b;
    do
      b = a, (b.flags & 4098) !== 0 && (c = b.return), a = b.return;
    while (a);
  }
  return b.tag === 3 ? c : null;
}
function Wb(a) {
  if (a.tag === 13) {
    var b = a.memoizedState;
    b === null && (a = a.alternate, a !== null && (b = a.memoizedState));
    if (b !== null)
      return b.dehydrated;
  }
  return null;
}
function Xb(a) {
  if (Vb(a) !== a)
    throw Error(p2(188));
}
function Yb(a) {
  var b = a.alternate;
  if (!b) {
    b = Vb(a);
    if (b === null)
      throw Error(p2(188));
    return b !== a ? null : a;
  }
  for (var c = a, d = b;; ) {
    var e = c.return;
    if (e === null)
      break;
    var f = e.alternate;
    if (f === null) {
      d = e.return;
      if (d !== null) {
        c = d;
        continue;
      }
      break;
    }
    if (e.child === f.child) {
      for (f = e.child;f; ) {
        if (f === c)
          return Xb(e), a;
        if (f === d)
          return Xb(e), b;
        f = f.sibling;
      }
      throw Error(p2(188));
    }
    if (c.return !== d.return)
      c = e, d = f;
    else {
      for (var g = false, h = e.child;h; ) {
        if (h === c) {
          g = true;
          c = e;
          d = f;
          break;
        }
        if (h === d) {
          g = true;
          d = e;
          c = f;
          break;
        }
        h = h.sibling;
      }
      if (!g) {
        for (h = f.child;h; ) {
          if (h === c) {
            g = true;
            c = f;
            d = e;
            break;
          }
          if (h === d) {
            g = true;
            d = f;
            c = e;
            break;
          }
          h = h.sibling;
        }
        if (!g)
          throw Error(p2(189));
      }
    }
    if (c.alternate !== d)
      throw Error(p2(190));
  }
  if (c.tag !== 3)
    throw Error(p2(188));
  return c.stateNode.current === c ? a : b;
}
function Zb(a) {
  a = Yb(a);
  return a !== null ? $b(a) : null;
}
function $b(a) {
  if (a.tag === 5 || a.tag === 6)
    return a;
  for (a = a.child;a !== null; ) {
    var b = $b(a);
    if (b !== null)
      return b;
    a = a.sibling;
  }
  return null;
}
function mc(a) {
  if (lc && typeof lc.onCommitFiberRoot === "function")
    try {
      lc.onCommitFiberRoot(kc, a, undefined, (a.current.flags & 128) === 128);
    } catch (b) {}
}
function nc(a) {
  a >>>= 0;
  return a === 0 ? 32 : 31 - (pc(a) / qc | 0) | 0;
}
function tc(a) {
  switch (a & -a) {
    case 1:
      return 1;
    case 2:
      return 2;
    case 4:
      return 4;
    case 8:
      return 8;
    case 16:
      return 16;
    case 32:
      return 32;
    case 64:
    case 128:
    case 256:
    case 512:
    case 1024:
    case 2048:
    case 4096:
    case 8192:
    case 16384:
    case 32768:
    case 65536:
    case 131072:
    case 262144:
    case 524288:
    case 1048576:
    case 2097152:
      return a & 4194240;
    case 4194304:
    case 8388608:
    case 16777216:
    case 33554432:
    case 67108864:
      return a & 130023424;
    case 134217728:
      return 134217728;
    case 268435456:
      return 268435456;
    case 536870912:
      return 536870912;
    case 1073741824:
      return 1073741824;
    default:
      return a;
  }
}
function uc(a, b) {
  var c = a.pendingLanes;
  if (c === 0)
    return 0;
  var d = 0, e = a.suspendedLanes, f = a.pingedLanes, g = c & 268435455;
  if (g !== 0) {
    var h = g & ~e;
    h !== 0 ? d = tc(h) : (f &= g, f !== 0 && (d = tc(f)));
  } else
    g = c & ~e, g !== 0 ? d = tc(g) : f !== 0 && (d = tc(f));
  if (d === 0)
    return 0;
  if (b !== 0 && b !== d && (b & e) === 0 && (e = d & -d, f = b & -b, e >= f || e === 16 && (f & 4194240) !== 0))
    return b;
  (d & 4) !== 0 && (d |= c & 16);
  b = a.entangledLanes;
  if (b !== 0)
    for (a = a.entanglements, b &= d;0 < b; )
      c = 31 - oc(b), e = 1 << c, d |= a[c], b &= ~e;
  return d;
}
function vc(a, b) {
  switch (a) {
    case 1:
    case 2:
    case 4:
      return b + 250;
    case 8:
    case 16:
    case 32:
    case 64:
    case 128:
    case 256:
    case 512:
    case 1024:
    case 2048:
    case 4096:
    case 8192:
    case 16384:
    case 32768:
    case 65536:
    case 131072:
    case 262144:
    case 524288:
    case 1048576:
    case 2097152:
      return b + 5000;
    case 4194304:
    case 8388608:
    case 16777216:
    case 33554432:
    case 67108864:
      return -1;
    case 134217728:
    case 268435456:
    case 536870912:
    case 1073741824:
      return -1;
    default:
      return -1;
  }
}
function wc(a, b) {
  for (var { suspendedLanes: c, pingedLanes: d, expirationTimes: e, pendingLanes: f } = a;0 < f; ) {
    var g = 31 - oc(f), h = 1 << g, k = e[g];
    if (k === -1) {
      if ((h & c) === 0 || (h & d) !== 0)
        e[g] = vc(h, b);
    } else
      k <= b && (a.expiredLanes |= h);
    f &= ~h;
  }
}
function xc(a) {
  a = a.pendingLanes & -1073741825;
  return a !== 0 ? a : a & 1073741824 ? 1073741824 : 0;
}
function yc() {
  var a = rc;
  rc <<= 1;
  (rc & 4194240) === 0 && (rc = 64);
  return a;
}
function zc(a) {
  for (var b = [], c = 0;31 > c; c++)
    b.push(a);
  return b;
}
function Ac(a, b, c) {
  a.pendingLanes |= b;
  b !== 536870912 && (a.suspendedLanes = 0, a.pingedLanes = 0);
  a = a.eventTimes;
  b = 31 - oc(b);
  a[b] = c;
}
function Bc(a, b) {
  var c = a.pendingLanes & ~b;
  a.pendingLanes = b;
  a.suspendedLanes = 0;
  a.pingedLanes = 0;
  a.expiredLanes &= b;
  a.mutableReadLanes &= b;
  a.entangledLanes &= b;
  b = a.entanglements;
  var d = a.eventTimes;
  for (a = a.expirationTimes;0 < c; ) {
    var e = 31 - oc(c), f = 1 << e;
    b[e] = 0;
    d[e] = -1;
    a[e] = -1;
    c &= ~f;
  }
}
function Cc(a, b) {
  var c = a.entangledLanes |= b;
  for (a = a.entanglements;c; ) {
    var d = 31 - oc(c), e = 1 << d;
    e & b | a[d] & b && (a[d] |= b);
    c &= ~e;
  }
}
function Dc(a) {
  a &= -a;
  return 1 < a ? 4 < a ? (a & 268435455) !== 0 ? 16 : 536870912 : 4 : 1;
}
function Sc(a, b) {
  switch (a) {
    case "focusin":
    case "focusout":
      Lc = null;
      break;
    case "dragenter":
    case "dragleave":
      Mc = null;
      break;
    case "mouseover":
    case "mouseout":
      Nc = null;
      break;
    case "pointerover":
    case "pointerout":
      Oc.delete(b.pointerId);
      break;
    case "gotpointercapture":
    case "lostpointercapture":
      Pc.delete(b.pointerId);
  }
}
function Tc(a, b, c, d, e, f) {
  if (a === null || a.nativeEvent !== f)
    return a = { blockedOn: b, domEventName: c, eventSystemFlags: d, nativeEvent: f, targetContainers: [e] }, b !== null && (b = Cb(b), b !== null && Fc(b)), a;
  a.eventSystemFlags |= d;
  b = a.targetContainers;
  e !== null && b.indexOf(e) === -1 && b.push(e);
  return a;
}
function Uc(a, b, c, d, e) {
  switch (b) {
    case "focusin":
      return Lc = Tc(Lc, a, b, c, d, e), true;
    case "dragenter":
      return Mc = Tc(Mc, a, b, c, d, e), true;
    case "mouseover":
      return Nc = Tc(Nc, a, b, c, d, e), true;
    case "pointerover":
      var f = e.pointerId;
      Oc.set(f, Tc(Oc.get(f) || null, a, b, c, d, e));
      return true;
    case "gotpointercapture":
      return f = e.pointerId, Pc.set(f, Tc(Pc.get(f) || null, a, b, c, d, e)), true;
  }
  return false;
}
function Vc(a) {
  var b = Wc(a.target);
  if (b !== null) {
    var c = Vb(b);
    if (c !== null) {
      if (b = c.tag, b === 13) {
        if (b = Wb(c), b !== null) {
          a.blockedOn = b;
          Ic(a.priority, function() {
            Gc(c);
          });
          return;
        }
      } else if (b === 3 && c.stateNode.current.memoizedState.isDehydrated) {
        a.blockedOn = c.tag === 3 ? c.stateNode.containerInfo : null;
        return;
      }
    }
  }
  a.blockedOn = null;
}
function Xc(a) {
  if (a.blockedOn !== null)
    return false;
  for (var b = a.targetContainers;0 < b.length; ) {
    var c = Yc(a.domEventName, a.eventSystemFlags, b[0], a.nativeEvent);
    if (c === null) {
      c = a.nativeEvent;
      var d = new c.constructor(c.type, c);
      wb = d;
      c.target.dispatchEvent(d);
      wb = null;
    } else
      return b = Cb(c), b !== null && Fc(b), a.blockedOn = c, false;
    b.shift();
  }
  return true;
}
function Zc(a, b, c) {
  Xc(a) && c.delete(b);
}
function $c() {
  Jc = false;
  Lc !== null && Xc(Lc) && (Lc = null);
  Mc !== null && Xc(Mc) && (Mc = null);
  Nc !== null && Xc(Nc) && (Nc = null);
  Oc.forEach(Zc);
  Pc.forEach(Zc);
}
function ad(a, b) {
  a.blockedOn === b && (a.blockedOn = null, Jc || (Jc = true, ca.unstable_scheduleCallback(ca.unstable_NormalPriority, $c)));
}
function bd(a) {
  function b(b2) {
    return ad(b2, a);
  }
  if (0 < Kc.length) {
    ad(Kc[0], a);
    for (var c = 1;c < Kc.length; c++) {
      var d = Kc[c];
      d.blockedOn === a && (d.blockedOn = null);
    }
  }
  Lc !== null && ad(Lc, a);
  Mc !== null && ad(Mc, a);
  Nc !== null && ad(Nc, a);
  Oc.forEach(b);
  Pc.forEach(b);
  for (c = 0;c < Qc.length; c++)
    d = Qc[c], d.blockedOn === a && (d.blockedOn = null);
  for (;0 < Qc.length && (c = Qc[0], c.blockedOn === null); )
    Vc(c), c.blockedOn === null && Qc.shift();
}
function ed(a, b, c, d) {
  var e = C2, f = cd.transition;
  cd.transition = null;
  try {
    C2 = 1, fd(a, b, c, d);
  } finally {
    C2 = e, cd.transition = f;
  }
}
function gd(a, b, c, d) {
  var e = C2, f = cd.transition;
  cd.transition = null;
  try {
    C2 = 4, fd(a, b, c, d);
  } finally {
    C2 = e, cd.transition = f;
  }
}
function fd(a, b, c, d) {
  if (dd) {
    var e = Yc(a, b, c, d);
    if (e === null)
      hd(a, b, d, id, c), Sc(a, d);
    else if (Uc(e, a, b, c, d))
      d.stopPropagation();
    else if (Sc(a, d), b & 4 && -1 < Rc.indexOf(a)) {
      for (;e !== null; ) {
        var f = Cb(e);
        f !== null && Ec(f);
        f = Yc(a, b, c, d);
        f === null && hd(a, b, d, id, c);
        if (f === e)
          break;
        e = f;
      }
      e !== null && d.stopPropagation();
    } else
      hd(a, b, d, null, c);
  }
}
function Yc(a, b, c, d) {
  id = null;
  a = xb(d);
  a = Wc(a);
  if (a !== null)
    if (b = Vb(a), b === null)
      a = null;
    else if (c = b.tag, c === 13) {
      a = Wb(b);
      if (a !== null)
        return a;
      a = null;
    } else if (c === 3) {
      if (b.stateNode.current.memoizedState.isDehydrated)
        return b.tag === 3 ? b.stateNode.containerInfo : null;
      a = null;
    } else
      b !== a && (a = null);
  id = a;
  return null;
}
function jd(a) {
  switch (a) {
    case "cancel":
    case "click":
    case "close":
    case "contextmenu":
    case "copy":
    case "cut":
    case "auxclick":
    case "dblclick":
    case "dragend":
    case "dragstart":
    case "drop":
    case "focusin":
    case "focusout":
    case "input":
    case "invalid":
    case "keydown":
    case "keypress":
    case "keyup":
    case "mousedown":
    case "mouseup":
    case "paste":
    case "pause":
    case "play":
    case "pointercancel":
    case "pointerdown":
    case "pointerup":
    case "ratechange":
    case "reset":
    case "resize":
    case "seeked":
    case "submit":
    case "touchcancel":
    case "touchend":
    case "touchstart":
    case "volumechange":
    case "change":
    case "selectionchange":
    case "textInput":
    case "compositionstart":
    case "compositionend":
    case "compositionupdate":
    case "beforeblur":
    case "afterblur":
    case "beforeinput":
    case "blur":
    case "fullscreenchange":
    case "focus":
    case "hashchange":
    case "popstate":
    case "select":
    case "selectstart":
      return 1;
    case "drag":
    case "dragenter":
    case "dragexit":
    case "dragleave":
    case "dragover":
    case "mousemove":
    case "mouseout":
    case "mouseover":
    case "pointermove":
    case "pointerout":
    case "pointerover":
    case "scroll":
    case "toggle":
    case "touchmove":
    case "wheel":
    case "mouseenter":
    case "mouseleave":
    case "pointerenter":
    case "pointerleave":
      return 4;
    case "message":
      switch (ec()) {
        case fc:
          return 1;
        case gc:
          return 4;
        case hc:
        case ic:
          return 16;
        case jc:
          return 536870912;
        default:
          return 16;
      }
    default:
      return 16;
  }
}
function nd() {
  if (md)
    return md;
  var a, b = ld, c = b.length, d, e = "value" in kd ? kd.value : kd.textContent, f = e.length;
  for (a = 0;a < c && b[a] === e[a]; a++)
    ;
  var g = c - a;
  for (d = 1;d <= g && b[c - d] === e[f - d]; d++)
    ;
  return md = e.slice(a, 1 < d ? 1 - d : undefined);
}
function od(a) {
  var b = a.keyCode;
  "charCode" in a ? (a = a.charCode, a === 0 && b === 13 && (a = 13)) : a = b;
  a === 10 && (a = 13);
  return 32 <= a || a === 13 ? a : 0;
}
function pd() {
  return true;
}
function qd() {
  return false;
}
function rd(a) {
  function b(b2, d, e, f, g) {
    this._reactName = b2;
    this._targetInst = e;
    this.type = d;
    this.nativeEvent = f;
    this.target = g;
    this.currentTarget = null;
    for (var c in a)
      a.hasOwnProperty(c) && (b2 = a[c], this[c] = b2 ? b2(f) : f[c]);
    this.isDefaultPrevented = (f.defaultPrevented != null ? f.defaultPrevented : f.returnValue === false) ? pd : qd;
    this.isPropagationStopped = qd;
    return this;
  }
  A2(b.prototype, { preventDefault: function() {
    this.defaultPrevented = true;
    var a2 = this.nativeEvent;
    a2 && (a2.preventDefault ? a2.preventDefault() : typeof a2.returnValue !== "unknown" && (a2.returnValue = false), this.isDefaultPrevented = pd);
  }, stopPropagation: function() {
    var a2 = this.nativeEvent;
    a2 && (a2.stopPropagation ? a2.stopPropagation() : typeof a2.cancelBubble !== "unknown" && (a2.cancelBubble = true), this.isPropagationStopped = pd);
  }, persist: function() {}, isPersistent: pd });
  return b;
}
function Pd(a) {
  var b = this.nativeEvent;
  return b.getModifierState ? b.getModifierState(a) : (a = Od[a]) ? !!b[a] : false;
}
function zd() {
  return Pd;
}
function ge(a, b) {
  switch (a) {
    case "keyup":
      return $d.indexOf(b.keyCode) !== -1;
    case "keydown":
      return b.keyCode !== 229;
    case "keypress":
    case "mousedown":
    case "focusout":
      return true;
    default:
      return false;
  }
}
function he(a) {
  a = a.detail;
  return typeof a === "object" && "data" in a ? a.data : null;
}
function je(a, b) {
  switch (a) {
    case "compositionend":
      return he(b);
    case "keypress":
      if (b.which !== 32)
        return null;
      fe = true;
      return ee;
    case "textInput":
      return a = b.data, a === ee && fe ? null : a;
    default:
      return null;
  }
}
function ke(a, b) {
  if (ie)
    return a === "compositionend" || !ae && ge(a, b) ? (a = nd(), md = ld = kd = null, ie = false, a) : null;
  switch (a) {
    case "paste":
      return null;
    case "keypress":
      if (!(b.ctrlKey || b.altKey || b.metaKey) || b.ctrlKey && b.altKey) {
        if (b.char && 1 < b.char.length)
          return b.char;
        if (b.which)
          return String.fromCharCode(b.which);
      }
      return null;
    case "compositionend":
      return de && b.locale !== "ko" ? null : b.data;
    default:
      return null;
  }
}
function me(a) {
  var b = a && a.nodeName && a.nodeName.toLowerCase();
  return b === "input" ? !!le[a.type] : b === "textarea" ? true : false;
}
function ne(a, b, c, d) {
  Eb(d);
  b = oe(b, "onChange");
  0 < b.length && (c = new td("onChange", "change", null, c, d), a.push({ event: c, listeners: b }));
}
function re(a) {
  se(a, 0);
}
function te(a) {
  var b = ue(a);
  if (Wa(b))
    return a;
}
function ve(a, b) {
  if (a === "change")
    return b;
}
function Ae() {
  pe && (pe.detachEvent("onpropertychange", Be), qe = pe = null);
}
function Be(a) {
  if (a.propertyName === "value" && te(qe)) {
    var b = [];
    ne(b, qe, a, xb(a));
    Jb(re, b);
  }
}
function Ce(a, b, c) {
  a === "focusin" ? (Ae(), pe = b, qe = c, pe.attachEvent("onpropertychange", Be)) : a === "focusout" && Ae();
}
function De(a) {
  if (a === "selectionchange" || a === "keyup" || a === "keydown")
    return te(qe);
}
function Ee(a, b) {
  if (a === "click")
    return te(b);
}
function Fe(a, b) {
  if (a === "input" || a === "change")
    return te(b);
}
function Ge(a, b) {
  return a === b && (a !== 0 || 1 / a === 1 / b) || a !== a && b !== b;
}
function Ie(a, b) {
  if (He(a, b))
    return true;
  if (typeof a !== "object" || a === null || typeof b !== "object" || b === null)
    return false;
  var c = Object.keys(a), d = Object.keys(b);
  if (c.length !== d.length)
    return false;
  for (d = 0;d < c.length; d++) {
    var e = c[d];
    if (!ja.call(b, e) || !He(a[e], b[e]))
      return false;
  }
  return true;
}
function Je(a) {
  for (;a && a.firstChild; )
    a = a.firstChild;
  return a;
}
function Ke(a, b) {
  var c = Je(a);
  a = 0;
  for (var d;c; ) {
    if (c.nodeType === 3) {
      d = a + c.textContent.length;
      if (a <= b && d >= b)
        return { node: c, offset: b - a };
      a = d;
    }
    a: {
      for (;c; ) {
        if (c.nextSibling) {
          c = c.nextSibling;
          break a;
        }
        c = c.parentNode;
      }
      c = undefined;
    }
    c = Je(c);
  }
}
function Le(a, b) {
  return a && b ? a === b ? true : a && a.nodeType === 3 ? false : b && b.nodeType === 3 ? Le(a, b.parentNode) : ("contains" in a) ? a.contains(b) : a.compareDocumentPosition ? !!(a.compareDocumentPosition(b) & 16) : false : false;
}
function Me() {
  for (var a = window, b = Xa();b instanceof a.HTMLIFrameElement; ) {
    try {
      var c = typeof b.contentWindow.location.href === "string";
    } catch (d) {
      c = false;
    }
    if (c)
      a = b.contentWindow;
    else
      break;
    b = Xa(a.document);
  }
  return b;
}
function Ne(a) {
  var b = a && a.nodeName && a.nodeName.toLowerCase();
  return b && (b === "input" && (a.type === "text" || a.type === "search" || a.type === "tel" || a.type === "url" || a.type === "password") || b === "textarea" || a.contentEditable === "true");
}
function Oe(a) {
  var b = Me(), c = a.focusedElem, d = a.selectionRange;
  if (b !== c && c && c.ownerDocument && Le(c.ownerDocument.documentElement, c)) {
    if (d !== null && Ne(c)) {
      if (b = d.start, a = d.end, a === undefined && (a = b), "selectionStart" in c)
        c.selectionStart = b, c.selectionEnd = Math.min(a, c.value.length);
      else if (a = (b = c.ownerDocument || document) && b.defaultView || window, a.getSelection) {
        a = a.getSelection();
        var e = c.textContent.length, f = Math.min(d.start, e);
        d = d.end === undefined ? f : Math.min(d.end, e);
        !a.extend && f > d && (e = d, d = f, f = e);
        e = Ke(c, f);
        var g = Ke(c, d);
        e && g && (a.rangeCount !== 1 || a.anchorNode !== e.node || a.anchorOffset !== e.offset || a.focusNode !== g.node || a.focusOffset !== g.offset) && (b = b.createRange(), b.setStart(e.node, e.offset), a.removeAllRanges(), f > d ? (a.addRange(b), a.extend(g.node, g.offset)) : (b.setEnd(g.node, g.offset), a.addRange(b)));
      }
    }
    b = [];
    for (a = c;a = a.parentNode; )
      a.nodeType === 1 && b.push({ element: a, left: a.scrollLeft, top: a.scrollTop });
    typeof c.focus === "function" && c.focus();
    for (c = 0;c < b.length; c++)
      a = b[c], a.element.scrollLeft = a.left, a.element.scrollTop = a.top;
  }
}
function Ue(a, b, c) {
  var d = c.window === c ? c.document : c.nodeType === 9 ? c : c.ownerDocument;
  Te || Qe == null || Qe !== Xa(d) || (d = Qe, ("selectionStart" in d) && Ne(d) ? d = { start: d.selectionStart, end: d.selectionEnd } : (d = (d.ownerDocument && d.ownerDocument.defaultView || window).getSelection(), d = { anchorNode: d.anchorNode, anchorOffset: d.anchorOffset, focusNode: d.focusNode, focusOffset: d.focusOffset }), Se && Ie(Se, d) || (Se = d, d = oe(Re, "onSelect"), 0 < d.length && (b = new td("onSelect", "select", null, b, c), a.push({ event: b, listeners: d }), b.target = Qe)));
}
function Ve(a, b) {
  var c = {};
  c[a.toLowerCase()] = b.toLowerCase();
  c["Webkit" + a] = "webkit" + b;
  c["Moz" + a] = "moz" + b;
  return c;
}
function Ze(a) {
  if (Xe[a])
    return Xe[a];
  if (!We[a])
    return a;
  var b = We[a], c;
  for (c in b)
    if (b.hasOwnProperty(c) && c in Ye)
      return Xe[a] = b[c];
  return a;
}
function ff(a, b) {
  df.set(a, b);
  fa(b, [a]);
}
function nf(a, b, c) {
  var d = a.type || "unknown-event";
  a.currentTarget = c;
  Ub(d, b, undefined, a);
  a.currentTarget = null;
}
function se(a, b) {
  b = (b & 4) !== 0;
  for (var c = 0;c < a.length; c++) {
    var d = a[c], e = d.event;
    d = d.listeners;
    a: {
      var f = undefined;
      if (b)
        for (var g = d.length - 1;0 <= g; g--) {
          var h = d[g], k = h.instance, l2 = h.currentTarget;
          h = h.listener;
          if (k !== f && e.isPropagationStopped())
            break a;
          nf(e, h, l2);
          f = k;
        }
      else
        for (g = 0;g < d.length; g++) {
          h = d[g];
          k = h.instance;
          l2 = h.currentTarget;
          h = h.listener;
          if (k !== f && e.isPropagationStopped())
            break a;
          nf(e, h, l2);
          f = k;
        }
    }
  }
  if (Qb)
    throw a = Rb, Qb = false, Rb = null, a;
}
function D2(a, b) {
  var c = b[of];
  c === undefined && (c = b[of] = new Set);
  var d = a + "__bubble";
  c.has(d) || (pf(b, a, 2, false), c.add(d));
}
function qf(a, b, c) {
  var d = 0;
  b && (d |= 4);
  pf(c, a, d, b);
}
function sf(a) {
  if (!a[rf]) {
    a[rf] = true;
    da.forEach(function(b2) {
      b2 !== "selectionchange" && (mf.has(b2) || qf(b2, false, a), qf(b2, true, a));
    });
    var b = a.nodeType === 9 ? a : a.ownerDocument;
    b === null || b[rf] || (b[rf] = true, qf("selectionchange", false, b));
  }
}
function pf(a, b, c, d) {
  switch (jd(b)) {
    case 1:
      var e = ed;
      break;
    case 4:
      e = gd;
      break;
    default:
      e = fd;
  }
  c = e.bind(null, b, c, a);
  e = undefined;
  !Lb || b !== "touchstart" && b !== "touchmove" && b !== "wheel" || (e = true);
  d ? e !== undefined ? a.addEventListener(b, c, { capture: true, passive: e }) : a.addEventListener(b, c, true) : e !== undefined ? a.addEventListener(b, c, { passive: e }) : a.addEventListener(b, c, false);
}
function hd(a, b, c, d, e) {
  var f = d;
  if ((b & 1) === 0 && (b & 2) === 0 && d !== null)
    a:
      for (;; ) {
        if (d === null)
          return;
        var g = d.tag;
        if (g === 3 || g === 4) {
          var h = d.stateNode.containerInfo;
          if (h === e || h.nodeType === 8 && h.parentNode === e)
            break;
          if (g === 4)
            for (g = d.return;g !== null; ) {
              var k = g.tag;
              if (k === 3 || k === 4) {
                if (k = g.stateNode.containerInfo, k === e || k.nodeType === 8 && k.parentNode === e)
                  return;
              }
              g = g.return;
            }
          for (;h !== null; ) {
            g = Wc(h);
            if (g === null)
              return;
            k = g.tag;
            if (k === 5 || k === 6) {
              d = f = g;
              continue a;
            }
            h = h.parentNode;
          }
        }
        d = d.return;
      }
  Jb(function() {
    var d2 = f, e2 = xb(c), g2 = [];
    a: {
      var h2 = df.get(a);
      if (h2 !== undefined) {
        var k2 = td, n2 = a;
        switch (a) {
          case "keypress":
            if (od(c) === 0)
              break a;
          case "keydown":
          case "keyup":
            k2 = Rd;
            break;
          case "focusin":
            n2 = "focus";
            k2 = Fd;
            break;
          case "focusout":
            n2 = "blur";
            k2 = Fd;
            break;
          case "beforeblur":
          case "afterblur":
            k2 = Fd;
            break;
          case "click":
            if (c.button === 2)
              break a;
          case "auxclick":
          case "dblclick":
          case "mousedown":
          case "mousemove":
          case "mouseup":
          case "mouseout":
          case "mouseover":
          case "contextmenu":
            k2 = Bd;
            break;
          case "drag":
          case "dragend":
          case "dragenter":
          case "dragexit":
          case "dragleave":
          case "dragover":
          case "dragstart":
          case "drop":
            k2 = Dd;
            break;
          case "touchcancel":
          case "touchend":
          case "touchmove":
          case "touchstart":
            k2 = Vd;
            break;
          case $e:
          case af:
          case bf:
            k2 = Hd;
            break;
          case cf:
            k2 = Xd;
            break;
          case "scroll":
            k2 = vd;
            break;
          case "wheel":
            k2 = Zd;
            break;
          case "copy":
          case "cut":
          case "paste":
            k2 = Jd;
            break;
          case "gotpointercapture":
          case "lostpointercapture":
          case "pointercancel":
          case "pointerdown":
          case "pointermove":
          case "pointerout":
          case "pointerover":
          case "pointerup":
            k2 = Td;
        }
        var t2 = (b & 4) !== 0, J2 = !t2 && a === "scroll", x2 = t2 ? h2 !== null ? h2 + "Capture" : null : h2;
        t2 = [];
        for (var w2 = d2, u2;w2 !== null; ) {
          u2 = w2;
          var F2 = u2.stateNode;
          u2.tag === 5 && F2 !== null && (u2 = F2, x2 !== null && (F2 = Kb(w2, x2), F2 != null && t2.push(tf(w2, F2, u2))));
          if (J2)
            break;
          w2 = w2.return;
        }
        0 < t2.length && (h2 = new k2(h2, n2, null, c, e2), g2.push({ event: h2, listeners: t2 }));
      }
    }
    if ((b & 7) === 0) {
      a: {
        h2 = a === "mouseover" || a === "pointerover";
        k2 = a === "mouseout" || a === "pointerout";
        if (h2 && c !== wb && (n2 = c.relatedTarget || c.fromElement) && (Wc(n2) || n2[uf]))
          break a;
        if (k2 || h2) {
          h2 = e2.window === e2 ? e2 : (h2 = e2.ownerDocument) ? h2.defaultView || h2.parentWindow : window;
          if (k2) {
            if (n2 = c.relatedTarget || c.toElement, k2 = d2, n2 = n2 ? Wc(n2) : null, n2 !== null && (J2 = Vb(n2), n2 !== J2 || n2.tag !== 5 && n2.tag !== 6))
              n2 = null;
          } else
            k2 = null, n2 = d2;
          if (k2 !== n2) {
            t2 = Bd;
            F2 = "onMouseLeave";
            x2 = "onMouseEnter";
            w2 = "mouse";
            if (a === "pointerout" || a === "pointerover")
              t2 = Td, F2 = "onPointerLeave", x2 = "onPointerEnter", w2 = "pointer";
            J2 = k2 == null ? h2 : ue(k2);
            u2 = n2 == null ? h2 : ue(n2);
            h2 = new t2(F2, w2 + "leave", k2, c, e2);
            h2.target = J2;
            h2.relatedTarget = u2;
            F2 = null;
            Wc(e2) === d2 && (t2 = new t2(x2, w2 + "enter", n2, c, e2), t2.target = u2, t2.relatedTarget = J2, F2 = t2);
            J2 = F2;
            if (k2 && n2)
              b: {
                t2 = k2;
                x2 = n2;
                w2 = 0;
                for (u2 = t2;u2; u2 = vf(u2))
                  w2++;
                u2 = 0;
                for (F2 = x2;F2; F2 = vf(F2))
                  u2++;
                for (;0 < w2 - u2; )
                  t2 = vf(t2), w2--;
                for (;0 < u2 - w2; )
                  x2 = vf(x2), u2--;
                for (;w2--; ) {
                  if (t2 === x2 || x2 !== null && t2 === x2.alternate)
                    break b;
                  t2 = vf(t2);
                  x2 = vf(x2);
                }
                t2 = null;
              }
            else
              t2 = null;
            k2 !== null && wf(g2, h2, k2, t2, false);
            n2 !== null && J2 !== null && wf(g2, J2, n2, t2, true);
          }
        }
      }
      a: {
        h2 = d2 ? ue(d2) : window;
        k2 = h2.nodeName && h2.nodeName.toLowerCase();
        if (k2 === "select" || k2 === "input" && h2.type === "file")
          var na = ve;
        else if (me(h2))
          if (we)
            na = Fe;
          else {
            na = De;
            var xa = Ce;
          }
        else
          (k2 = h2.nodeName) && k2.toLowerCase() === "input" && (h2.type === "checkbox" || h2.type === "radio") && (na = Ee);
        if (na && (na = na(a, d2))) {
          ne(g2, na, c, e2);
          break a;
        }
        xa && xa(a, h2, d2);
        a === "focusout" && (xa = h2._wrapperState) && xa.controlled && h2.type === "number" && cb(h2, "number", h2.value);
      }
      xa = d2 ? ue(d2) : window;
      switch (a) {
        case "focusin":
          if (me(xa) || xa.contentEditable === "true")
            Qe = xa, Re = d2, Se = null;
          break;
        case "focusout":
          Se = Re = Qe = null;
          break;
        case "mousedown":
          Te = true;
          break;
        case "contextmenu":
        case "mouseup":
        case "dragend":
          Te = false;
          Ue(g2, c, e2);
          break;
        case "selectionchange":
          if (Pe)
            break;
        case "keydown":
        case "keyup":
          Ue(g2, c, e2);
      }
      var $a;
      if (ae)
        b: {
          switch (a) {
            case "compositionstart":
              var ba = "onCompositionStart";
              break b;
            case "compositionend":
              ba = "onCompositionEnd";
              break b;
            case "compositionupdate":
              ba = "onCompositionUpdate";
              break b;
          }
          ba = undefined;
        }
      else
        ie ? ge(a, c) && (ba = "onCompositionEnd") : a === "keydown" && c.keyCode === 229 && (ba = "onCompositionStart");
      ba && (de && c.locale !== "ko" && (ie || ba !== "onCompositionStart" ? ba === "onCompositionEnd" && ie && ($a = nd()) : (kd = e2, ld = ("value" in kd) ? kd.value : kd.textContent, ie = true)), xa = oe(d2, ba), 0 < xa.length && (ba = new Ld(ba, a, null, c, e2), g2.push({ event: ba, listeners: xa }), $a ? ba.data = $a : ($a = he(c), $a !== null && (ba.data = $a))));
      if ($a = ce ? je(a, c) : ke(a, c))
        d2 = oe(d2, "onBeforeInput"), 0 < d2.length && (e2 = new Ld("onBeforeInput", "beforeinput", null, c, e2), g2.push({ event: e2, listeners: d2 }), e2.data = $a);
    }
    se(g2, b);
  });
}
function tf(a, b, c) {
  return { instance: a, listener: b, currentTarget: c };
}
function oe(a, b) {
  for (var c = b + "Capture", d = [];a !== null; ) {
    var e = a, f = e.stateNode;
    e.tag === 5 && f !== null && (e = f, f = Kb(a, c), f != null && d.unshift(tf(a, f, e)), f = Kb(a, b), f != null && d.push(tf(a, f, e)));
    a = a.return;
  }
  return d;
}
function vf(a) {
  if (a === null)
    return null;
  do
    a = a.return;
  while (a && a.tag !== 5);
  return a ? a : null;
}
function wf(a, b, c, d, e) {
  for (var f = b._reactName, g = [];c !== null && c !== d; ) {
    var h = c, k = h.alternate, l2 = h.stateNode;
    if (k !== null && k === d)
      break;
    h.tag === 5 && l2 !== null && (h = l2, e ? (k = Kb(c, f), k != null && g.unshift(tf(c, k, h))) : e || (k = Kb(c, f), k != null && g.push(tf(c, k, h))));
    c = c.return;
  }
  g.length !== 0 && a.push({ event: b, listeners: g });
}
function zf(a) {
  return (typeof a === "string" ? a : "" + a).replace(xf, `
`).replace(yf, "");
}
function Af(a, b, c) {
  b = zf(b);
  if (zf(a) !== b && c)
    throw Error(p2(425));
}
function Bf() {}
function Ef(a, b) {
  return a === "textarea" || a === "noscript" || typeof b.children === "string" || typeof b.children === "number" || typeof b.dangerouslySetInnerHTML === "object" && b.dangerouslySetInnerHTML !== null && b.dangerouslySetInnerHTML.__html != null;
}
function If(a) {
  setTimeout(function() {
    throw a;
  });
}
function Kf(a, b) {
  var c = b, d = 0;
  do {
    var e = c.nextSibling;
    a.removeChild(c);
    if (e && e.nodeType === 8)
      if (c = e.data, c === "/$") {
        if (d === 0) {
          a.removeChild(e);
          bd(b);
          return;
        }
        d--;
      } else
        c !== "$" && c !== "$?" && c !== "$!" || d++;
    c = e;
  } while (c);
  bd(b);
}
function Lf(a) {
  for (;a != null; a = a.nextSibling) {
    var b = a.nodeType;
    if (b === 1 || b === 3)
      break;
    if (b === 8) {
      b = a.data;
      if (b === "$" || b === "$!" || b === "$?")
        break;
      if (b === "/$")
        return null;
    }
  }
  return a;
}
function Mf(a) {
  a = a.previousSibling;
  for (var b = 0;a; ) {
    if (a.nodeType === 8) {
      var c = a.data;
      if (c === "$" || c === "$!" || c === "$?") {
        if (b === 0)
          return a;
        b--;
      } else
        c === "/$" && b++;
    }
    a = a.previousSibling;
  }
  return null;
}
function Wc(a) {
  var b = a[Of];
  if (b)
    return b;
  for (var c = a.parentNode;c; ) {
    if (b = c[uf] || c[Of]) {
      c = b.alternate;
      if (b.child !== null || c !== null && c.child !== null)
        for (a = Mf(a);a !== null; ) {
          if (c = a[Of])
            return c;
          a = Mf(a);
        }
      return b;
    }
    a = c;
    c = a.parentNode;
  }
  return null;
}
function Cb(a) {
  a = a[Of] || a[uf];
  return !a || a.tag !== 5 && a.tag !== 6 && a.tag !== 13 && a.tag !== 3 ? null : a;
}
function ue(a) {
  if (a.tag === 5 || a.tag === 6)
    return a.stateNode;
  throw Error(p2(33));
}
function Db(a) {
  return a[Pf] || null;
}
function Uf(a) {
  return { current: a };
}
function E2(a) {
  0 > Tf || (a.current = Sf[Tf], Sf[Tf] = null, Tf--);
}
function G2(a, b) {
  Tf++;
  Sf[Tf] = a.current;
  a.current = b;
}
function Yf(a, b) {
  var c = a.type.contextTypes;
  if (!c)
    return Vf;
  var d = a.stateNode;
  if (d && d.__reactInternalMemoizedUnmaskedChildContext === b)
    return d.__reactInternalMemoizedMaskedChildContext;
  var e = {}, f;
  for (f in c)
    e[f] = b[f];
  d && (a = a.stateNode, a.__reactInternalMemoizedUnmaskedChildContext = b, a.__reactInternalMemoizedMaskedChildContext = e);
  return e;
}
function Zf(a) {
  a = a.childContextTypes;
  return a !== null && a !== undefined;
}
function $f() {
  E2(Wf);
  E2(H2);
}
function ag(a, b, c) {
  if (H2.current !== Vf)
    throw Error(p2(168));
  G2(H2, b);
  G2(Wf, c);
}
function bg(a, b, c) {
  var d = a.stateNode;
  b = b.childContextTypes;
  if (typeof d.getChildContext !== "function")
    return c;
  d = d.getChildContext();
  for (var e in d)
    if (!(e in b))
      throw Error(p2(108, Ra(a) || "Unknown", e));
  return A2({}, c, d);
}
function cg(a) {
  a = (a = a.stateNode) && a.__reactInternalMemoizedMergedChildContext || Vf;
  Xf = H2.current;
  G2(H2, a);
  G2(Wf, Wf.current);
  return true;
}
function dg(a, b, c) {
  var d = a.stateNode;
  if (!d)
    throw Error(p2(169));
  c ? (a = bg(a, b, Xf), d.__reactInternalMemoizedMergedChildContext = a, E2(Wf), E2(H2), G2(H2, a)) : E2(Wf);
  G2(Wf, c);
}
function hg(a) {
  eg === null ? eg = [a] : eg.push(a);
}
function ig(a) {
  fg = true;
  hg(a);
}
function jg() {
  if (!gg && eg !== null) {
    gg = true;
    var a = 0, b = C2;
    try {
      var c = eg;
      for (C2 = 1;a < c.length; a++) {
        var d = c[a];
        do
          d = d(true);
        while (d !== null);
      }
      eg = null;
      fg = false;
    } catch (e) {
      throw eg !== null && (eg = eg.slice(a + 1)), ac(fc, jg), e;
    } finally {
      C2 = b, gg = false;
    }
  }
  return null;
}
function tg(a, b) {
  kg[lg++] = ng;
  kg[lg++] = mg;
  mg = a;
  ng = b;
}
function ug(a, b, c) {
  og[pg++] = rg;
  og[pg++] = sg;
  og[pg++] = qg;
  qg = a;
  var d = rg;
  a = sg;
  var e = 32 - oc(d) - 1;
  d &= ~(1 << e);
  c += 1;
  var f = 32 - oc(b) + e;
  if (30 < f) {
    var g = e - e % 5;
    f = (d & (1 << g) - 1).toString(32);
    d >>= g;
    e -= g;
    rg = 1 << 32 - oc(b) + e | c << e | d;
    sg = f + a;
  } else
    rg = 1 << f | c << e | d, sg = a;
}
function vg(a) {
  a.return !== null && (tg(a, 1), ug(a, 1, 0));
}
function wg(a) {
  for (;a === mg; )
    mg = kg[--lg], kg[lg] = null, ng = kg[--lg], kg[lg] = null;
  for (;a === qg; )
    qg = og[--pg], og[pg] = null, sg = og[--pg], og[pg] = null, rg = og[--pg], og[pg] = null;
}
function Ag(a, b) {
  var c = Bg(5, null, null, 0);
  c.elementType = "DELETED";
  c.stateNode = b;
  c.return = a;
  b = a.deletions;
  b === null ? (a.deletions = [c], a.flags |= 16) : b.push(c);
}
function Cg(a, b) {
  switch (a.tag) {
    case 5:
      var c = a.type;
      b = b.nodeType !== 1 || c.toLowerCase() !== b.nodeName.toLowerCase() ? null : b;
      return b !== null ? (a.stateNode = b, xg = a, yg = Lf(b.firstChild), true) : false;
    case 6:
      return b = a.pendingProps === "" || b.nodeType !== 3 ? null : b, b !== null ? (a.stateNode = b, xg = a, yg = null, true) : false;
    case 13:
      return b = b.nodeType !== 8 ? null : b, b !== null ? (c = qg !== null ? { id: rg, overflow: sg } : null, a.memoizedState = { dehydrated: b, treeContext: c, retryLane: 1073741824 }, c = Bg(18, null, null, 0), c.stateNode = b, c.return = a, a.child = c, xg = a, yg = null, true) : false;
    default:
      return false;
  }
}
function Dg(a) {
  return (a.mode & 1) !== 0 && (a.flags & 128) === 0;
}
function Eg(a) {
  if (I2) {
    var b = yg;
    if (b) {
      var c = b;
      if (!Cg(a, b)) {
        if (Dg(a))
          throw Error(p2(418));
        b = Lf(c.nextSibling);
        var d = xg;
        b && Cg(a, b) ? Ag(d, c) : (a.flags = a.flags & -4097 | 2, I2 = false, xg = a);
      }
    } else {
      if (Dg(a))
        throw Error(p2(418));
      a.flags = a.flags & -4097 | 2;
      I2 = false;
      xg = a;
    }
  }
}
function Fg(a) {
  for (a = a.return;a !== null && a.tag !== 5 && a.tag !== 3 && a.tag !== 13; )
    a = a.return;
  xg = a;
}
function Gg(a) {
  if (a !== xg)
    return false;
  if (!I2)
    return Fg(a), I2 = true, false;
  var b;
  (b = a.tag !== 3) && !(b = a.tag !== 5) && (b = a.type, b = b !== "head" && b !== "body" && !Ef(a.type, a.memoizedProps));
  if (b && (b = yg)) {
    if (Dg(a))
      throw Hg(), Error(p2(418));
    for (;b; )
      Ag(a, b), b = Lf(b.nextSibling);
  }
  Fg(a);
  if (a.tag === 13) {
    a = a.memoizedState;
    a = a !== null ? a.dehydrated : null;
    if (!a)
      throw Error(p2(317));
    a: {
      a = a.nextSibling;
      for (b = 0;a; ) {
        if (a.nodeType === 8) {
          var c = a.data;
          if (c === "/$") {
            if (b === 0) {
              yg = Lf(a.nextSibling);
              break a;
            }
            b--;
          } else
            c !== "$" && c !== "$!" && c !== "$?" || b++;
        }
        a = a.nextSibling;
      }
      yg = null;
    }
  } else
    yg = xg ? Lf(a.stateNode.nextSibling) : null;
  return true;
}
function Hg() {
  for (var a = yg;a; )
    a = Lf(a.nextSibling);
}
function Ig() {
  yg = xg = null;
  I2 = false;
}
function Jg(a) {
  zg === null ? zg = [a] : zg.push(a);
}
function Lg(a, b, c) {
  a = c.ref;
  if (a !== null && typeof a !== "function" && typeof a !== "object") {
    if (c._owner) {
      c = c._owner;
      if (c) {
        if (c.tag !== 1)
          throw Error(p2(309));
        var d = c.stateNode;
      }
      if (!d)
        throw Error(p2(147, a));
      var e = d, f = "" + a;
      if (b !== null && b.ref !== null && typeof b.ref === "function" && b.ref._stringRef === f)
        return b.ref;
      b = function(a2) {
        var b2 = e.refs;
        a2 === null ? delete b2[f] : b2[f] = a2;
      };
      b._stringRef = f;
      return b;
    }
    if (typeof a !== "string")
      throw Error(p2(284));
    if (!c._owner)
      throw Error(p2(290, a));
  }
  return a;
}
function Mg(a, b) {
  a = Object.prototype.toString.call(b);
  throw Error(p2(31, a === "[object Object]" ? "object with keys {" + Object.keys(b).join(", ") + "}" : a));
}
function Ng(a) {
  var b = a._init;
  return b(a._payload);
}
function Og(a) {
  function b(b2, c2) {
    if (a) {
      var d2 = b2.deletions;
      d2 === null ? (b2.deletions = [c2], b2.flags |= 16) : d2.push(c2);
    }
  }
  function c(c2, d2) {
    if (!a)
      return null;
    for (;d2 !== null; )
      b(c2, d2), d2 = d2.sibling;
    return null;
  }
  function d(a2, b2) {
    for (a2 = new Map;b2 !== null; )
      b2.key !== null ? a2.set(b2.key, b2) : a2.set(b2.index, b2), b2 = b2.sibling;
    return a2;
  }
  function e(a2, b2) {
    a2 = Pg(a2, b2);
    a2.index = 0;
    a2.sibling = null;
    return a2;
  }
  function f(b2, c2, d2) {
    b2.index = d2;
    if (!a)
      return b2.flags |= 1048576, c2;
    d2 = b2.alternate;
    if (d2 !== null)
      return d2 = d2.index, d2 < c2 ? (b2.flags |= 2, c2) : d2;
    b2.flags |= 2;
    return c2;
  }
  function g(b2) {
    a && b2.alternate === null && (b2.flags |= 2);
    return b2;
  }
  function h(a2, b2, c2, d2) {
    if (b2 === null || b2.tag !== 6)
      return b2 = Qg(c2, a2.mode, d2), b2.return = a2, b2;
    b2 = e(b2, c2);
    b2.return = a2;
    return b2;
  }
  function k(a2, b2, c2, d2) {
    var f2 = c2.type;
    if (f2 === ya)
      return m(a2, b2, c2.props.children, d2, c2.key);
    if (b2 !== null && (b2.elementType === f2 || typeof f2 === "object" && f2 !== null && f2.$$typeof === Ha && Ng(f2) === b2.type))
      return d2 = e(b2, c2.props), d2.ref = Lg(a2, b2, c2), d2.return = a2, d2;
    d2 = Rg(c2.type, c2.key, c2.props, null, a2.mode, d2);
    d2.ref = Lg(a2, b2, c2);
    d2.return = a2;
    return d2;
  }
  function l2(a2, b2, c2, d2) {
    if (b2 === null || b2.tag !== 4 || b2.stateNode.containerInfo !== c2.containerInfo || b2.stateNode.implementation !== c2.implementation)
      return b2 = Sg(c2, a2.mode, d2), b2.return = a2, b2;
    b2 = e(b2, c2.children || []);
    b2.return = a2;
    return b2;
  }
  function m(a2, b2, c2, d2, f2) {
    if (b2 === null || b2.tag !== 7)
      return b2 = Tg(c2, a2.mode, d2, f2), b2.return = a2, b2;
    b2 = e(b2, c2);
    b2.return = a2;
    return b2;
  }
  function q2(a2, b2, c2) {
    if (typeof b2 === "string" && b2 !== "" || typeof b2 === "number")
      return b2 = Qg("" + b2, a2.mode, c2), b2.return = a2, b2;
    if (typeof b2 === "object" && b2 !== null) {
      switch (b2.$$typeof) {
        case va:
          return c2 = Rg(b2.type, b2.key, b2.props, null, a2.mode, c2), c2.ref = Lg(a2, null, b2), c2.return = a2, c2;
        case wa:
          return b2 = Sg(b2, a2.mode, c2), b2.return = a2, b2;
        case Ha:
          var d2 = b2._init;
          return q2(a2, d2(b2._payload), c2);
      }
      if (eb(b2) || Ka(b2))
        return b2 = Tg(b2, a2.mode, c2, null), b2.return = a2, b2;
      Mg(a2, b2);
    }
    return null;
  }
  function r2(a2, b2, c2, d2) {
    var e2 = b2 !== null ? b2.key : null;
    if (typeof c2 === "string" && c2 !== "" || typeof c2 === "number")
      return e2 !== null ? null : h(a2, b2, "" + c2, d2);
    if (typeof c2 === "object" && c2 !== null) {
      switch (c2.$$typeof) {
        case va:
          return c2.key === e2 ? k(a2, b2, c2, d2) : null;
        case wa:
          return c2.key === e2 ? l2(a2, b2, c2, d2) : null;
        case Ha:
          return e2 = c2._init, r2(a2, b2, e2(c2._payload), d2);
      }
      if (eb(c2) || Ka(c2))
        return e2 !== null ? null : m(a2, b2, c2, d2, null);
      Mg(a2, c2);
    }
    return null;
  }
  function y2(a2, b2, c2, d2, e2) {
    if (typeof d2 === "string" && d2 !== "" || typeof d2 === "number")
      return a2 = a2.get(c2) || null, h(b2, a2, "" + d2, e2);
    if (typeof d2 === "object" && d2 !== null) {
      switch (d2.$$typeof) {
        case va:
          return a2 = a2.get(d2.key === null ? c2 : d2.key) || null, k(b2, a2, d2, e2);
        case wa:
          return a2 = a2.get(d2.key === null ? c2 : d2.key) || null, l2(b2, a2, d2, e2);
        case Ha:
          var f2 = d2._init;
          return y2(a2, b2, c2, f2(d2._payload), e2);
      }
      if (eb(d2) || Ka(d2))
        return a2 = a2.get(c2) || null, m(b2, a2, d2, e2, null);
      Mg(b2, d2);
    }
    return null;
  }
  function n2(e2, g2, h2, k2) {
    for (var l3 = null, m2 = null, u2 = g2, w2 = g2 = 0, x2 = null;u2 !== null && w2 < h2.length; w2++) {
      u2.index > w2 ? (x2 = u2, u2 = null) : x2 = u2.sibling;
      var n3 = r2(e2, u2, h2[w2], k2);
      if (n3 === null) {
        u2 === null && (u2 = x2);
        break;
      }
      a && u2 && n3.alternate === null && b(e2, u2);
      g2 = f(n3, g2, w2);
      m2 === null ? l3 = n3 : m2.sibling = n3;
      m2 = n3;
      u2 = x2;
    }
    if (w2 === h2.length)
      return c(e2, u2), I2 && tg(e2, w2), l3;
    if (u2 === null) {
      for (;w2 < h2.length; w2++)
        u2 = q2(e2, h2[w2], k2), u2 !== null && (g2 = f(u2, g2, w2), m2 === null ? l3 = u2 : m2.sibling = u2, m2 = u2);
      I2 && tg(e2, w2);
      return l3;
    }
    for (u2 = d(e2, u2);w2 < h2.length; w2++)
      x2 = y2(u2, e2, w2, h2[w2], k2), x2 !== null && (a && x2.alternate !== null && u2.delete(x2.key === null ? w2 : x2.key), g2 = f(x2, g2, w2), m2 === null ? l3 = x2 : m2.sibling = x2, m2 = x2);
    a && u2.forEach(function(a2) {
      return b(e2, a2);
    });
    I2 && tg(e2, w2);
    return l3;
  }
  function t2(e2, g2, h2, k2) {
    var l3 = Ka(h2);
    if (typeof l3 !== "function")
      throw Error(p2(150));
    h2 = l3.call(h2);
    if (h2 == null)
      throw Error(p2(151));
    for (var u2 = l3 = null, m2 = g2, w2 = g2 = 0, x2 = null, n3 = h2.next();m2 !== null && !n3.done; w2++, n3 = h2.next()) {
      m2.index > w2 ? (x2 = m2, m2 = null) : x2 = m2.sibling;
      var t3 = r2(e2, m2, n3.value, k2);
      if (t3 === null) {
        m2 === null && (m2 = x2);
        break;
      }
      a && m2 && t3.alternate === null && b(e2, m2);
      g2 = f(t3, g2, w2);
      u2 === null ? l3 = t3 : u2.sibling = t3;
      u2 = t3;
      m2 = x2;
    }
    if (n3.done)
      return c(e2, m2), I2 && tg(e2, w2), l3;
    if (m2 === null) {
      for (;!n3.done; w2++, n3 = h2.next())
        n3 = q2(e2, n3.value, k2), n3 !== null && (g2 = f(n3, g2, w2), u2 === null ? l3 = n3 : u2.sibling = n3, u2 = n3);
      I2 && tg(e2, w2);
      return l3;
    }
    for (m2 = d(e2, m2);!n3.done; w2++, n3 = h2.next())
      n3 = y2(m2, e2, w2, n3.value, k2), n3 !== null && (a && n3.alternate !== null && m2.delete(n3.key === null ? w2 : n3.key), g2 = f(n3, g2, w2), u2 === null ? l3 = n3 : u2.sibling = n3, u2 = n3);
    a && m2.forEach(function(a2) {
      return b(e2, a2);
    });
    I2 && tg(e2, w2);
    return l3;
  }
  function J2(a2, d2, f2, h2) {
    typeof f2 === "object" && f2 !== null && f2.type === ya && f2.key === null && (f2 = f2.props.children);
    if (typeof f2 === "object" && f2 !== null) {
      switch (f2.$$typeof) {
        case va:
          a: {
            for (var k2 = f2.key, l3 = d2;l3 !== null; ) {
              if (l3.key === k2) {
                k2 = f2.type;
                if (k2 === ya) {
                  if (l3.tag === 7) {
                    c(a2, l3.sibling);
                    d2 = e(l3, f2.props.children);
                    d2.return = a2;
                    a2 = d2;
                    break a;
                  }
                } else if (l3.elementType === k2 || typeof k2 === "object" && k2 !== null && k2.$$typeof === Ha && Ng(k2) === l3.type) {
                  c(a2, l3.sibling);
                  d2 = e(l3, f2.props);
                  d2.ref = Lg(a2, l3, f2);
                  d2.return = a2;
                  a2 = d2;
                  break a;
                }
                c(a2, l3);
                break;
              } else
                b(a2, l3);
              l3 = l3.sibling;
            }
            f2.type === ya ? (d2 = Tg(f2.props.children, a2.mode, h2, f2.key), d2.return = a2, a2 = d2) : (h2 = Rg(f2.type, f2.key, f2.props, null, a2.mode, h2), h2.ref = Lg(a2, d2, f2), h2.return = a2, a2 = h2);
          }
          return g(a2);
        case wa:
          a: {
            for (l3 = f2.key;d2 !== null; ) {
              if (d2.key === l3)
                if (d2.tag === 4 && d2.stateNode.containerInfo === f2.containerInfo && d2.stateNode.implementation === f2.implementation) {
                  c(a2, d2.sibling);
                  d2 = e(d2, f2.children || []);
                  d2.return = a2;
                  a2 = d2;
                  break a;
                } else {
                  c(a2, d2);
                  break;
                }
              else
                b(a2, d2);
              d2 = d2.sibling;
            }
            d2 = Sg(f2, a2.mode, h2);
            d2.return = a2;
            a2 = d2;
          }
          return g(a2);
        case Ha:
          return l3 = f2._init, J2(a2, d2, l3(f2._payload), h2);
      }
      if (eb(f2))
        return n2(a2, d2, f2, h2);
      if (Ka(f2))
        return t2(a2, d2, f2, h2);
      Mg(a2, f2);
    }
    return typeof f2 === "string" && f2 !== "" || typeof f2 === "number" ? (f2 = "" + f2, d2 !== null && d2.tag === 6 ? (c(a2, d2.sibling), d2 = e(d2, f2), d2.return = a2, a2 = d2) : (c(a2, d2), d2 = Qg(f2, a2.mode, h2), d2.return = a2, a2 = d2), g(a2)) : c(a2, d2);
  }
  return J2;
}
function $g() {
  Zg = Yg = Xg = null;
}
function ah(a) {
  var b = Wg.current;
  E2(Wg);
  a._currentValue = b;
}
function bh(a, b, c) {
  for (;a !== null; ) {
    var d = a.alternate;
    (a.childLanes & b) !== b ? (a.childLanes |= b, d !== null && (d.childLanes |= b)) : d !== null && (d.childLanes & b) !== b && (d.childLanes |= b);
    if (a === c)
      break;
    a = a.return;
  }
}
function ch(a, b) {
  Xg = a;
  Zg = Yg = null;
  a = a.dependencies;
  a !== null && a.firstContext !== null && ((a.lanes & b) !== 0 && (dh = true), a.firstContext = null);
}
function eh(a) {
  var b = a._currentValue;
  if (Zg !== a)
    if (a = { context: a, memoizedValue: b, next: null }, Yg === null) {
      if (Xg === null)
        throw Error(p2(308));
      Yg = a;
      Xg.dependencies = { lanes: 0, firstContext: a };
    } else
      Yg = Yg.next = a;
  return b;
}
function gh(a) {
  fh === null ? fh = [a] : fh.push(a);
}
function hh(a, b, c, d) {
  var e = b.interleaved;
  e === null ? (c.next = c, gh(b)) : (c.next = e.next, e.next = c);
  b.interleaved = c;
  return ih(a, d);
}
function ih(a, b) {
  a.lanes |= b;
  var c = a.alternate;
  c !== null && (c.lanes |= b);
  c = a;
  for (a = a.return;a !== null; )
    a.childLanes |= b, c = a.alternate, c !== null && (c.childLanes |= b), c = a, a = a.return;
  return c.tag === 3 ? c.stateNode : null;
}
function kh(a) {
  a.updateQueue = { baseState: a.memoizedState, firstBaseUpdate: null, lastBaseUpdate: null, shared: { pending: null, interleaved: null, lanes: 0 }, effects: null };
}
function lh(a, b) {
  a = a.updateQueue;
  b.updateQueue === a && (b.updateQueue = { baseState: a.baseState, firstBaseUpdate: a.firstBaseUpdate, lastBaseUpdate: a.lastBaseUpdate, shared: a.shared, effects: a.effects });
}
function mh(a, b) {
  return { eventTime: a, lane: b, tag: 0, payload: null, callback: null, next: null };
}
function nh(a, b, c) {
  var d = a.updateQueue;
  if (d === null)
    return null;
  d = d.shared;
  if ((K2 & 2) !== 0) {
    var e = d.pending;
    e === null ? b.next = b : (b.next = e.next, e.next = b);
    d.pending = b;
    return ih(a, c);
  }
  e = d.interleaved;
  e === null ? (b.next = b, gh(d)) : (b.next = e.next, e.next = b);
  d.interleaved = b;
  return ih(a, c);
}
function oh(a, b, c) {
  b = b.updateQueue;
  if (b !== null && (b = b.shared, (c & 4194240) !== 0)) {
    var d = b.lanes;
    d &= a.pendingLanes;
    c |= d;
    b.lanes = c;
    Cc(a, c);
  }
}
function ph(a, b) {
  var { updateQueue: c, alternate: d } = a;
  if (d !== null && (d = d.updateQueue, c === d)) {
    var e = null, f = null;
    c = c.firstBaseUpdate;
    if (c !== null) {
      do {
        var g = { eventTime: c.eventTime, lane: c.lane, tag: c.tag, payload: c.payload, callback: c.callback, next: null };
        f === null ? e = f = g : f = f.next = g;
        c = c.next;
      } while (c !== null);
      f === null ? e = f = b : f = f.next = b;
    } else
      e = f = b;
    c = { baseState: d.baseState, firstBaseUpdate: e, lastBaseUpdate: f, shared: d.shared, effects: d.effects };
    a.updateQueue = c;
    return;
  }
  a = c.lastBaseUpdate;
  a === null ? c.firstBaseUpdate = b : a.next = b;
  c.lastBaseUpdate = b;
}
function qh(a, b, c, d) {
  var e = a.updateQueue;
  jh = false;
  var { firstBaseUpdate: f, lastBaseUpdate: g } = e, h = e.shared.pending;
  if (h !== null) {
    e.shared.pending = null;
    var k = h, l2 = k.next;
    k.next = null;
    g === null ? f = l2 : g.next = l2;
    g = k;
    var m = a.alternate;
    m !== null && (m = m.updateQueue, h = m.lastBaseUpdate, h !== g && (h === null ? m.firstBaseUpdate = l2 : h.next = l2, m.lastBaseUpdate = k));
  }
  if (f !== null) {
    var q2 = e.baseState;
    g = 0;
    m = l2 = k = null;
    h = f;
    do {
      var { lane: r2, eventTime: y2 } = h;
      if ((d & r2) === r2) {
        m !== null && (m = m.next = {
          eventTime: y2,
          lane: 0,
          tag: h.tag,
          payload: h.payload,
          callback: h.callback,
          next: null
        });
        a: {
          var n2 = a, t2 = h;
          r2 = b;
          y2 = c;
          switch (t2.tag) {
            case 1:
              n2 = t2.payload;
              if (typeof n2 === "function") {
                q2 = n2.call(y2, q2, r2);
                break a;
              }
              q2 = n2;
              break a;
            case 3:
              n2.flags = n2.flags & -65537 | 128;
            case 0:
              n2 = t2.payload;
              r2 = typeof n2 === "function" ? n2.call(y2, q2, r2) : n2;
              if (r2 === null || r2 === undefined)
                break a;
              q2 = A2({}, q2, r2);
              break a;
            case 2:
              jh = true;
          }
        }
        h.callback !== null && h.lane !== 0 && (a.flags |= 64, r2 = e.effects, r2 === null ? e.effects = [h] : r2.push(h));
      } else
        y2 = { eventTime: y2, lane: r2, tag: h.tag, payload: h.payload, callback: h.callback, next: null }, m === null ? (l2 = m = y2, k = q2) : m = m.next = y2, g |= r2;
      h = h.next;
      if (h === null)
        if (h = e.shared.pending, h === null)
          break;
        else
          r2 = h, h = r2.next, r2.next = null, e.lastBaseUpdate = r2, e.shared.pending = null;
    } while (1);
    m === null && (k = q2);
    e.baseState = k;
    e.firstBaseUpdate = l2;
    e.lastBaseUpdate = m;
    b = e.shared.interleaved;
    if (b !== null) {
      e = b;
      do
        g |= e.lane, e = e.next;
      while (e !== b);
    } else
      f === null && (e.shared.lanes = 0);
    rh |= g;
    a.lanes = g;
    a.memoizedState = q2;
  }
}
function sh(a, b, c) {
  a = b.effects;
  b.effects = null;
  if (a !== null)
    for (b = 0;b < a.length; b++) {
      var d = a[b], e = d.callback;
      if (e !== null) {
        d.callback = null;
        d = c;
        if (typeof e !== "function")
          throw Error(p2(191, e));
        e.call(d);
      }
    }
}
function xh(a) {
  if (a === th)
    throw Error(p2(174));
  return a;
}
function yh(a, b) {
  G2(wh, b);
  G2(vh, a);
  G2(uh, th);
  a = b.nodeType;
  switch (a) {
    case 9:
    case 11:
      b = (b = b.documentElement) ? b.namespaceURI : lb(null, "");
      break;
    default:
      a = a === 8 ? b.parentNode : b, b = a.namespaceURI || null, a = a.tagName, b = lb(b, a);
  }
  E2(uh);
  G2(uh, b);
}
function zh() {
  E2(uh);
  E2(vh);
  E2(wh);
}
function Ah(a) {
  xh(wh.current);
  var b = xh(uh.current);
  var c = lb(b, a.type);
  b !== c && (G2(vh, a), G2(uh, c));
}
function Bh(a) {
  vh.current === a && (E2(uh), E2(vh));
}
function Ch(a) {
  for (var b = a;b !== null; ) {
    if (b.tag === 13) {
      var c = b.memoizedState;
      if (c !== null && (c = c.dehydrated, c === null || c.data === "$?" || c.data === "$!"))
        return b;
    } else if (b.tag === 19 && b.memoizedProps.revealOrder !== undefined) {
      if ((b.flags & 128) !== 0)
        return b;
    } else if (b.child !== null) {
      b.child.return = b;
      b = b.child;
      continue;
    }
    if (b === a)
      break;
    for (;b.sibling === null; ) {
      if (b.return === null || b.return === a)
        return null;
      b = b.return;
    }
    b.sibling.return = b.return;
    b = b.sibling;
  }
  return null;
}
function Eh() {
  for (var a = 0;a < Dh.length; a++)
    Dh[a]._workInProgressVersionPrimary = null;
  Dh.length = 0;
}
function P2() {
  throw Error(p2(321));
}
function Mh(a, b) {
  if (b === null)
    return false;
  for (var c = 0;c < b.length && c < a.length; c++)
    if (!He(a[c], b[c]))
      return false;
  return true;
}
function Nh(a, b, c, d, e, f) {
  Hh = f;
  M2 = b;
  b.memoizedState = null;
  b.updateQueue = null;
  b.lanes = 0;
  Fh.current = a === null || a.memoizedState === null ? Oh : Ph;
  a = c(d, e);
  if (Jh) {
    f = 0;
    do {
      Jh = false;
      Kh = 0;
      if (25 <= f)
        throw Error(p2(301));
      f += 1;
      O2 = N2 = null;
      b.updateQueue = null;
      Fh.current = Qh;
      a = c(d, e);
    } while (Jh);
  }
  Fh.current = Rh;
  b = N2 !== null && N2.next !== null;
  Hh = 0;
  O2 = N2 = M2 = null;
  Ih = false;
  if (b)
    throw Error(p2(300));
  return a;
}
function Sh() {
  var a = Kh !== 0;
  Kh = 0;
  return a;
}
function Th() {
  var a = { memoizedState: null, baseState: null, baseQueue: null, queue: null, next: null };
  O2 === null ? M2.memoizedState = O2 = a : O2 = O2.next = a;
  return O2;
}
function Uh() {
  if (N2 === null) {
    var a = M2.alternate;
    a = a !== null ? a.memoizedState : null;
  } else
    a = N2.next;
  var b = O2 === null ? M2.memoizedState : O2.next;
  if (b !== null)
    O2 = b, N2 = a;
  else {
    if (a === null)
      throw Error(p2(310));
    N2 = a;
    a = { memoizedState: N2.memoizedState, baseState: N2.baseState, baseQueue: N2.baseQueue, queue: N2.queue, next: null };
    O2 === null ? M2.memoizedState = O2 = a : O2 = O2.next = a;
  }
  return O2;
}
function Vh(a, b) {
  return typeof b === "function" ? b(a) : b;
}
function Wh(a) {
  var b = Uh(), c = b.queue;
  if (c === null)
    throw Error(p2(311));
  c.lastRenderedReducer = a;
  var d = N2, e = d.baseQueue, f = c.pending;
  if (f !== null) {
    if (e !== null) {
      var g = e.next;
      e.next = f.next;
      f.next = g;
    }
    d.baseQueue = e = f;
    c.pending = null;
  }
  if (e !== null) {
    f = e.next;
    d = d.baseState;
    var h = g = null, k = null, l2 = f;
    do {
      var m = l2.lane;
      if ((Hh & m) === m)
        k !== null && (k = k.next = { lane: 0, action: l2.action, hasEagerState: l2.hasEagerState, eagerState: l2.eagerState, next: null }), d = l2.hasEagerState ? l2.eagerState : a(d, l2.action);
      else {
        var q2 = {
          lane: m,
          action: l2.action,
          hasEagerState: l2.hasEagerState,
          eagerState: l2.eagerState,
          next: null
        };
        k === null ? (h = k = q2, g = d) : k = k.next = q2;
        M2.lanes |= m;
        rh |= m;
      }
      l2 = l2.next;
    } while (l2 !== null && l2 !== f);
    k === null ? g = d : k.next = h;
    He(d, b.memoizedState) || (dh = true);
    b.memoizedState = d;
    b.baseState = g;
    b.baseQueue = k;
    c.lastRenderedState = d;
  }
  a = c.interleaved;
  if (a !== null) {
    e = a;
    do
      f = e.lane, M2.lanes |= f, rh |= f, e = e.next;
    while (e !== a);
  } else
    e === null && (c.lanes = 0);
  return [b.memoizedState, c.dispatch];
}
function Xh(a) {
  var b = Uh(), c = b.queue;
  if (c === null)
    throw Error(p2(311));
  c.lastRenderedReducer = a;
  var { dispatch: d, pending: e } = c, f = b.memoizedState;
  if (e !== null) {
    c.pending = null;
    var g = e = e.next;
    do
      f = a(f, g.action), g = g.next;
    while (g !== e);
    He(f, b.memoizedState) || (dh = true);
    b.memoizedState = f;
    b.baseQueue === null && (b.baseState = f);
    c.lastRenderedState = f;
  }
  return [f, d];
}
function Yh() {}
function Zh(a, b) {
  var c = M2, d = Uh(), e = b(), f = !He(d.memoizedState, e);
  f && (d.memoizedState = e, dh = true);
  d = d.queue;
  $h(ai.bind(null, c, d, a), [a]);
  if (d.getSnapshot !== b || f || O2 !== null && O2.memoizedState.tag & 1) {
    c.flags |= 2048;
    bi(9, ci.bind(null, c, d, e, b), undefined, null);
    if (Q2 === null)
      throw Error(p2(349));
    (Hh & 30) !== 0 || di(c, b, e);
  }
  return e;
}
function di(a, b, c) {
  a.flags |= 16384;
  a = { getSnapshot: b, value: c };
  b = M2.updateQueue;
  b === null ? (b = { lastEffect: null, stores: null }, M2.updateQueue = b, b.stores = [a]) : (c = b.stores, c === null ? b.stores = [a] : c.push(a));
}
function ci(a, b, c, d) {
  b.value = c;
  b.getSnapshot = d;
  ei(b) && fi(a);
}
function ai(a, b, c) {
  return c(function() {
    ei(b) && fi(a);
  });
}
function ei(a) {
  var b = a.getSnapshot;
  a = a.value;
  try {
    var c = b();
    return !He(a, c);
  } catch (d) {
    return true;
  }
}
function fi(a) {
  var b = ih(a, 1);
  b !== null && gi(b, a, 1, -1);
}
function hi(a) {
  var b = Th();
  typeof a === "function" && (a = a());
  b.memoizedState = b.baseState = a;
  a = { pending: null, interleaved: null, lanes: 0, dispatch: null, lastRenderedReducer: Vh, lastRenderedState: a };
  b.queue = a;
  a = a.dispatch = ii.bind(null, M2, a);
  return [b.memoizedState, a];
}
function bi(a, b, c, d) {
  a = { tag: a, create: b, destroy: c, deps: d, next: null };
  b = M2.updateQueue;
  b === null ? (b = { lastEffect: null, stores: null }, M2.updateQueue = b, b.lastEffect = a.next = a) : (c = b.lastEffect, c === null ? b.lastEffect = a.next = a : (d = c.next, c.next = a, a.next = d, b.lastEffect = a));
  return a;
}
function ji() {
  return Uh().memoizedState;
}
function ki(a, b, c, d) {
  var e = Th();
  M2.flags |= a;
  e.memoizedState = bi(1 | b, c, undefined, d === undefined ? null : d);
}
function li(a, b, c, d) {
  var e = Uh();
  d = d === undefined ? null : d;
  var f = undefined;
  if (N2 !== null) {
    var g = N2.memoizedState;
    f = g.destroy;
    if (d !== null && Mh(d, g.deps)) {
      e.memoizedState = bi(b, c, f, d);
      return;
    }
  }
  M2.flags |= a;
  e.memoizedState = bi(1 | b, c, f, d);
}
function mi(a, b) {
  return ki(8390656, 8, a, b);
}
function $h(a, b) {
  return li(2048, 8, a, b);
}
function ni(a, b) {
  return li(4, 2, a, b);
}
function oi(a, b) {
  return li(4, 4, a, b);
}
function pi(a, b) {
  if (typeof b === "function")
    return a = a(), b(a), function() {
      b(null);
    };
  if (b !== null && b !== undefined)
    return a = a(), b.current = a, function() {
      b.current = null;
    };
}
function qi(a, b, c) {
  c = c !== null && c !== undefined ? c.concat([a]) : null;
  return li(4, 4, pi.bind(null, b, a), c);
}
function ri() {}
function si(a, b) {
  var c = Uh();
  b = b === undefined ? null : b;
  var d = c.memoizedState;
  if (d !== null && b !== null && Mh(b, d[1]))
    return d[0];
  c.memoizedState = [a, b];
  return a;
}
function ti(a, b) {
  var c = Uh();
  b = b === undefined ? null : b;
  var d = c.memoizedState;
  if (d !== null && b !== null && Mh(b, d[1]))
    return d[0];
  a = a();
  c.memoizedState = [a, b];
  return a;
}
function ui(a, b, c) {
  if ((Hh & 21) === 0)
    return a.baseState && (a.baseState = false, dh = true), a.memoizedState = c;
  He(c, b) || (c = yc(), M2.lanes |= c, rh |= c, a.baseState = true);
  return b;
}
function vi(a, b) {
  var c = C2;
  C2 = c !== 0 && 4 > c ? c : 4;
  a(true);
  var d = Gh.transition;
  Gh.transition = {};
  try {
    a(false), b();
  } finally {
    C2 = c, Gh.transition = d;
  }
}
function wi() {
  return Uh().memoizedState;
}
function xi(a, b, c) {
  var d = yi(a);
  c = { lane: d, action: c, hasEagerState: false, eagerState: null, next: null };
  if (zi(a))
    Ai(b, c);
  else if (c = hh(a, b, c, d), c !== null) {
    var e = R2();
    gi(c, a, d, e);
    Bi(c, b, d);
  }
}
function ii(a, b, c) {
  var d = yi(a), e = { lane: d, action: c, hasEagerState: false, eagerState: null, next: null };
  if (zi(a))
    Ai(b, e);
  else {
    var f = a.alternate;
    if (a.lanes === 0 && (f === null || f.lanes === 0) && (f = b.lastRenderedReducer, f !== null))
      try {
        var g = b.lastRenderedState, h = f(g, c);
        e.hasEagerState = true;
        e.eagerState = h;
        if (He(h, g)) {
          var k = b.interleaved;
          k === null ? (e.next = e, gh(b)) : (e.next = k.next, k.next = e);
          b.interleaved = e;
          return;
        }
      } catch (l2) {} finally {}
    c = hh(a, b, e, d);
    c !== null && (e = R2(), gi(c, a, d, e), Bi(c, b, d));
  }
}
function zi(a) {
  var b = a.alternate;
  return a === M2 || b !== null && b === M2;
}
function Ai(a, b) {
  Jh = Ih = true;
  var c = a.pending;
  c === null ? b.next = b : (b.next = c.next, c.next = b);
  a.pending = b;
}
function Bi(a, b, c) {
  if ((c & 4194240) !== 0) {
    var d = b.lanes;
    d &= a.pendingLanes;
    c |= d;
    b.lanes = c;
    Cc(a, c);
  }
}
function Ci(a, b) {
  if (a && a.defaultProps) {
    b = A2({}, b);
    a = a.defaultProps;
    for (var c in a)
      b[c] === undefined && (b[c] = a[c]);
    return b;
  }
  return b;
}
function Di(a, b, c, d) {
  b = a.memoizedState;
  c = c(d, b);
  c = c === null || c === undefined ? b : A2({}, b, c);
  a.memoizedState = c;
  a.lanes === 0 && (a.updateQueue.baseState = c);
}
function Fi(a, b, c, d, e, f, g) {
  a = a.stateNode;
  return typeof a.shouldComponentUpdate === "function" ? a.shouldComponentUpdate(d, f, g) : b.prototype && b.prototype.isPureReactComponent ? !Ie(c, d) || !Ie(e, f) : true;
}
function Gi(a, b, c) {
  var d = false, e = Vf;
  var f = b.contextType;
  typeof f === "object" && f !== null ? f = eh(f) : (e = Zf(b) ? Xf : H2.current, d = b.contextTypes, f = (d = d !== null && d !== undefined) ? Yf(a, e) : Vf);
  b = new b(c, f);
  a.memoizedState = b.state !== null && b.state !== undefined ? b.state : null;
  b.updater = Ei;
  a.stateNode = b;
  b._reactInternals = a;
  d && (a = a.stateNode, a.__reactInternalMemoizedUnmaskedChildContext = e, a.__reactInternalMemoizedMaskedChildContext = f);
  return b;
}
function Hi(a, b, c, d) {
  a = b.state;
  typeof b.componentWillReceiveProps === "function" && b.componentWillReceiveProps(c, d);
  typeof b.UNSAFE_componentWillReceiveProps === "function" && b.UNSAFE_componentWillReceiveProps(c, d);
  b.state !== a && Ei.enqueueReplaceState(b, b.state, null);
}
function Ii(a, b, c, d) {
  var e = a.stateNode;
  e.props = c;
  e.state = a.memoizedState;
  e.refs = {};
  kh(a);
  var f = b.contextType;
  typeof f === "object" && f !== null ? e.context = eh(f) : (f = Zf(b) ? Xf : H2.current, e.context = Yf(a, f));
  e.state = a.memoizedState;
  f = b.getDerivedStateFromProps;
  typeof f === "function" && (Di(a, b, f, c), e.state = a.memoizedState);
  typeof b.getDerivedStateFromProps === "function" || typeof e.getSnapshotBeforeUpdate === "function" || typeof e.UNSAFE_componentWillMount !== "function" && typeof e.componentWillMount !== "function" || (b = e.state, typeof e.componentWillMount === "function" && e.componentWillMount(), typeof e.UNSAFE_componentWillMount === "function" && e.UNSAFE_componentWillMount(), b !== e.state && Ei.enqueueReplaceState(e, e.state, null), qh(a, c, e, d), e.state = a.memoizedState);
  typeof e.componentDidMount === "function" && (a.flags |= 4194308);
}
function Ji(a, b) {
  try {
    var c = "", d = b;
    do
      c += Pa(d), d = d.return;
    while (d);
    var e = c;
  } catch (f) {
    e = `
Error generating stack: ` + f.message + `
` + f.stack;
  }
  return { value: a, source: b, stack: e, digest: null };
}
function Ki(a, b, c) {
  return { value: a, source: null, stack: c != null ? c : null, digest: b != null ? b : null };
}
function Li(a, b) {
  try {
    console.error(b.value);
  } catch (c) {
    setTimeout(function() {
      throw c;
    });
  }
}
function Ni(a, b, c) {
  c = mh(-1, c);
  c.tag = 3;
  c.payload = { element: null };
  var d = b.value;
  c.callback = function() {
    Oi || (Oi = true, Pi = d);
    Li(a, b);
  };
  return c;
}
function Qi(a, b, c) {
  c = mh(-1, c);
  c.tag = 3;
  var d = a.type.getDerivedStateFromError;
  if (typeof d === "function") {
    var e = b.value;
    c.payload = function() {
      return d(e);
    };
    c.callback = function() {
      Li(a, b);
    };
  }
  var f = a.stateNode;
  f !== null && typeof f.componentDidCatch === "function" && (c.callback = function() {
    Li(a, b);
    typeof d !== "function" && (Ri === null ? Ri = new Set([this]) : Ri.add(this));
    var c2 = b.stack;
    this.componentDidCatch(b.value, { componentStack: c2 !== null ? c2 : "" });
  });
  return c;
}
function Si(a, b, c) {
  var d = a.pingCache;
  if (d === null) {
    d = a.pingCache = new Mi;
    var e = new Set;
    d.set(b, e);
  } else
    e = d.get(b), e === undefined && (e = new Set, d.set(b, e));
  e.has(c) || (e.add(c), a = Ti.bind(null, a, b, c), b.then(a, a));
}
function Ui(a) {
  do {
    var b;
    if (b = a.tag === 13)
      b = a.memoizedState, b = b !== null ? b.dehydrated !== null ? true : false : true;
    if (b)
      return a;
    a = a.return;
  } while (a !== null);
  return null;
}
function Vi(a, b, c, d, e) {
  if ((a.mode & 1) === 0)
    return a === b ? a.flags |= 65536 : (a.flags |= 128, c.flags |= 131072, c.flags &= -52805, c.tag === 1 && (c.alternate === null ? c.tag = 17 : (b = mh(-1, 1), b.tag = 2, nh(c, b, 1))), c.lanes |= 1), a;
  a.flags |= 65536;
  a.lanes = e;
  return a;
}
function Xi(a, b, c, d) {
  b.child = a === null ? Vg(b, null, c, d) : Ug(b, a.child, c, d);
}
function Yi(a, b, c, d, e) {
  c = c.render;
  var f = b.ref;
  ch(b, e);
  d = Nh(a, b, c, d, f, e);
  c = Sh();
  if (a !== null && !dh)
    return b.updateQueue = a.updateQueue, b.flags &= -2053, a.lanes &= ~e, Zi(a, b, e);
  I2 && c && vg(b);
  b.flags |= 1;
  Xi(a, b, d, e);
  return b.child;
}
function $i(a, b, c, d, e) {
  if (a === null) {
    var f = c.type;
    if (typeof f === "function" && !aj(f) && f.defaultProps === undefined && c.compare === null && c.defaultProps === undefined)
      return b.tag = 15, b.type = f, bj(a, b, f, d, e);
    a = Rg(c.type, null, d, b, b.mode, e);
    a.ref = b.ref;
    a.return = b;
    return b.child = a;
  }
  f = a.child;
  if ((a.lanes & e) === 0) {
    var g = f.memoizedProps;
    c = c.compare;
    c = c !== null ? c : Ie;
    if (c(g, d) && a.ref === b.ref)
      return Zi(a, b, e);
  }
  b.flags |= 1;
  a = Pg(f, d);
  a.ref = b.ref;
  a.return = b;
  return b.child = a;
}
function bj(a, b, c, d, e) {
  if (a !== null) {
    var f = a.memoizedProps;
    if (Ie(f, d) && a.ref === b.ref)
      if (dh = false, b.pendingProps = d = f, (a.lanes & e) !== 0)
        (a.flags & 131072) !== 0 && (dh = true);
      else
        return b.lanes = a.lanes, Zi(a, b, e);
  }
  return cj(a, b, c, d, e);
}
function dj(a, b, c) {
  var d = b.pendingProps, e = d.children, f = a !== null ? a.memoizedState : null;
  if (d.mode === "hidden")
    if ((b.mode & 1) === 0)
      b.memoizedState = { baseLanes: 0, cachePool: null, transitions: null }, G2(ej, fj), fj |= c;
    else {
      if ((c & 1073741824) === 0)
        return a = f !== null ? f.baseLanes | c : c, b.lanes = b.childLanes = 1073741824, b.memoizedState = { baseLanes: a, cachePool: null, transitions: null }, b.updateQueue = null, G2(ej, fj), fj |= a, null;
      b.memoizedState = { baseLanes: 0, cachePool: null, transitions: null };
      d = f !== null ? f.baseLanes : c;
      G2(ej, fj);
      fj |= d;
    }
  else
    f !== null ? (d = f.baseLanes | c, b.memoizedState = null) : d = c, G2(ej, fj), fj |= d;
  Xi(a, b, e, c);
  return b.child;
}
function gj(a, b) {
  var c = b.ref;
  if (a === null && c !== null || a !== null && a.ref !== c)
    b.flags |= 512, b.flags |= 2097152;
}
function cj(a, b, c, d, e) {
  var f = Zf(c) ? Xf : H2.current;
  f = Yf(b, f);
  ch(b, e);
  c = Nh(a, b, c, d, f, e);
  d = Sh();
  if (a !== null && !dh)
    return b.updateQueue = a.updateQueue, b.flags &= -2053, a.lanes &= ~e, Zi(a, b, e);
  I2 && d && vg(b);
  b.flags |= 1;
  Xi(a, b, c, e);
  return b.child;
}
function hj(a, b, c, d, e) {
  if (Zf(c)) {
    var f = true;
    cg(b);
  } else
    f = false;
  ch(b, e);
  if (b.stateNode === null)
    ij(a, b), Gi(b, c, d), Ii(b, c, d, e), d = true;
  else if (a === null) {
    var { stateNode: g, memoizedProps: h } = b;
    g.props = h;
    var k = g.context, l2 = c.contextType;
    typeof l2 === "object" && l2 !== null ? l2 = eh(l2) : (l2 = Zf(c) ? Xf : H2.current, l2 = Yf(b, l2));
    var m = c.getDerivedStateFromProps, q2 = typeof m === "function" || typeof g.getSnapshotBeforeUpdate === "function";
    q2 || typeof g.UNSAFE_componentWillReceiveProps !== "function" && typeof g.componentWillReceiveProps !== "function" || (h !== d || k !== l2) && Hi(b, g, d, l2);
    jh = false;
    var r2 = b.memoizedState;
    g.state = r2;
    qh(b, d, g, e);
    k = b.memoizedState;
    h !== d || r2 !== k || Wf.current || jh ? (typeof m === "function" && (Di(b, c, m, d), k = b.memoizedState), (h = jh || Fi(b, c, h, d, r2, k, l2)) ? (q2 || typeof g.UNSAFE_componentWillMount !== "function" && typeof g.componentWillMount !== "function" || (typeof g.componentWillMount === "function" && g.componentWillMount(), typeof g.UNSAFE_componentWillMount === "function" && g.UNSAFE_componentWillMount()), typeof g.componentDidMount === "function" && (b.flags |= 4194308)) : (typeof g.componentDidMount === "function" && (b.flags |= 4194308), b.memoizedProps = d, b.memoizedState = k), g.props = d, g.state = k, g.context = l2, d = h) : (typeof g.componentDidMount === "function" && (b.flags |= 4194308), d = false);
  } else {
    g = b.stateNode;
    lh(a, b);
    h = b.memoizedProps;
    l2 = b.type === b.elementType ? h : Ci(b.type, h);
    g.props = l2;
    q2 = b.pendingProps;
    r2 = g.context;
    k = c.contextType;
    typeof k === "object" && k !== null ? k = eh(k) : (k = Zf(c) ? Xf : H2.current, k = Yf(b, k));
    var y2 = c.getDerivedStateFromProps;
    (m = typeof y2 === "function" || typeof g.getSnapshotBeforeUpdate === "function") || typeof g.UNSAFE_componentWillReceiveProps !== "function" && typeof g.componentWillReceiveProps !== "function" || (h !== q2 || r2 !== k) && Hi(b, g, d, k);
    jh = false;
    r2 = b.memoizedState;
    g.state = r2;
    qh(b, d, g, e);
    var n2 = b.memoizedState;
    h !== q2 || r2 !== n2 || Wf.current || jh ? (typeof y2 === "function" && (Di(b, c, y2, d), n2 = b.memoizedState), (l2 = jh || Fi(b, c, l2, d, r2, n2, k) || false) ? (m || typeof g.UNSAFE_componentWillUpdate !== "function" && typeof g.componentWillUpdate !== "function" || (typeof g.componentWillUpdate === "function" && g.componentWillUpdate(d, n2, k), typeof g.UNSAFE_componentWillUpdate === "function" && g.UNSAFE_componentWillUpdate(d, n2, k)), typeof g.componentDidUpdate === "function" && (b.flags |= 4), typeof g.getSnapshotBeforeUpdate === "function" && (b.flags |= 1024)) : (typeof g.componentDidUpdate !== "function" || h === a.memoizedProps && r2 === a.memoizedState || (b.flags |= 4), typeof g.getSnapshotBeforeUpdate !== "function" || h === a.memoizedProps && r2 === a.memoizedState || (b.flags |= 1024), b.memoizedProps = d, b.memoizedState = n2), g.props = d, g.state = n2, g.context = k, d = l2) : (typeof g.componentDidUpdate !== "function" || h === a.memoizedProps && r2 === a.memoizedState || (b.flags |= 4), typeof g.getSnapshotBeforeUpdate !== "function" || h === a.memoizedProps && r2 === a.memoizedState || (b.flags |= 1024), d = false);
  }
  return jj(a, b, c, d, f, e);
}
function jj(a, b, c, d, e, f) {
  gj(a, b);
  var g = (b.flags & 128) !== 0;
  if (!d && !g)
    return e && dg(b, c, false), Zi(a, b, f);
  d = b.stateNode;
  Wi.current = b;
  var h = g && typeof c.getDerivedStateFromError !== "function" ? null : d.render();
  b.flags |= 1;
  a !== null && g ? (b.child = Ug(b, a.child, null, f), b.child = Ug(b, null, h, f)) : Xi(a, b, h, f);
  b.memoizedState = d.state;
  e && dg(b, c, true);
  return b.child;
}
function kj(a) {
  var b = a.stateNode;
  b.pendingContext ? ag(a, b.pendingContext, b.pendingContext !== b.context) : b.context && ag(a, b.context, false);
  yh(a, b.containerInfo);
}
function lj(a, b, c, d, e) {
  Ig();
  Jg(e);
  b.flags |= 256;
  Xi(a, b, c, d);
  return b.child;
}
function nj(a) {
  return { baseLanes: a, cachePool: null, transitions: null };
}
function oj(a, b, c) {
  var d = b.pendingProps, e = L2.current, f = false, g = (b.flags & 128) !== 0, h;
  (h = g) || (h = a !== null && a.memoizedState === null ? false : (e & 2) !== 0);
  if (h)
    f = true, b.flags &= -129;
  else if (a === null || a.memoizedState !== null)
    e |= 1;
  G2(L2, e & 1);
  if (a === null) {
    Eg(b);
    a = b.memoizedState;
    if (a !== null && (a = a.dehydrated, a !== null))
      return (b.mode & 1) === 0 ? b.lanes = 1 : a.data === "$!" ? b.lanes = 8 : b.lanes = 1073741824, null;
    g = d.children;
    a = d.fallback;
    return f ? (d = b.mode, f = b.child, g = { mode: "hidden", children: g }, (d & 1) === 0 && f !== null ? (f.childLanes = 0, f.pendingProps = g) : f = pj(g, d, 0, null), a = Tg(a, d, c, null), f.return = b, a.return = b, f.sibling = a, b.child = f, b.child.memoizedState = nj(c), b.memoizedState = mj, a) : qj(b, g);
  }
  e = a.memoizedState;
  if (e !== null && (h = e.dehydrated, h !== null))
    return rj(a, b, g, d, h, e, c);
  if (f) {
    f = d.fallback;
    g = b.mode;
    e = a.child;
    h = e.sibling;
    var k = { mode: "hidden", children: d.children };
    (g & 1) === 0 && b.child !== e ? (d = b.child, d.childLanes = 0, d.pendingProps = k, b.deletions = null) : (d = Pg(e, k), d.subtreeFlags = e.subtreeFlags & 14680064);
    h !== null ? f = Pg(h, f) : (f = Tg(f, g, c, null), f.flags |= 2);
    f.return = b;
    d.return = b;
    d.sibling = f;
    b.child = d;
    d = f;
    f = b.child;
    g = a.child.memoizedState;
    g = g === null ? nj(c) : { baseLanes: g.baseLanes | c, cachePool: null, transitions: g.transitions };
    f.memoizedState = g;
    f.childLanes = a.childLanes & ~c;
    b.memoizedState = mj;
    return d;
  }
  f = a.child;
  a = f.sibling;
  d = Pg(f, { mode: "visible", children: d.children });
  (b.mode & 1) === 0 && (d.lanes = c);
  d.return = b;
  d.sibling = null;
  a !== null && (c = b.deletions, c === null ? (b.deletions = [a], b.flags |= 16) : c.push(a));
  b.child = d;
  b.memoizedState = null;
  return d;
}
function qj(a, b) {
  b = pj({ mode: "visible", children: b }, a.mode, 0, null);
  b.return = a;
  return a.child = b;
}
function sj(a, b, c, d) {
  d !== null && Jg(d);
  Ug(b, a.child, null, c);
  a = qj(b, b.pendingProps.children);
  a.flags |= 2;
  b.memoizedState = null;
  return a;
}
function rj(a, b, c, d, e, f, g) {
  if (c) {
    if (b.flags & 256)
      return b.flags &= -257, d = Ki(Error(p2(422))), sj(a, b, g, d);
    if (b.memoizedState !== null)
      return b.child = a.child, b.flags |= 128, null;
    f = d.fallback;
    e = b.mode;
    d = pj({ mode: "visible", children: d.children }, e, 0, null);
    f = Tg(f, e, g, null);
    f.flags |= 2;
    d.return = b;
    f.return = b;
    d.sibling = f;
    b.child = d;
    (b.mode & 1) !== 0 && Ug(b, a.child, null, g);
    b.child.memoizedState = nj(g);
    b.memoizedState = mj;
    return f;
  }
  if ((b.mode & 1) === 0)
    return sj(a, b, g, null);
  if (e.data === "$!") {
    d = e.nextSibling && e.nextSibling.dataset;
    if (d)
      var h = d.dgst;
    d = h;
    f = Error(p2(419));
    d = Ki(f, d, undefined);
    return sj(a, b, g, d);
  }
  h = (g & a.childLanes) !== 0;
  if (dh || h) {
    d = Q2;
    if (d !== null) {
      switch (g & -g) {
        case 4:
          e = 2;
          break;
        case 16:
          e = 8;
          break;
        case 64:
        case 128:
        case 256:
        case 512:
        case 1024:
        case 2048:
        case 4096:
        case 8192:
        case 16384:
        case 32768:
        case 65536:
        case 131072:
        case 262144:
        case 524288:
        case 1048576:
        case 2097152:
        case 4194304:
        case 8388608:
        case 16777216:
        case 33554432:
        case 67108864:
          e = 32;
          break;
        case 536870912:
          e = 268435456;
          break;
        default:
          e = 0;
      }
      e = (e & (d.suspendedLanes | g)) !== 0 ? 0 : e;
      e !== 0 && e !== f.retryLane && (f.retryLane = e, ih(a, e), gi(d, a, e, -1));
    }
    tj();
    d = Ki(Error(p2(421)));
    return sj(a, b, g, d);
  }
  if (e.data === "$?")
    return b.flags |= 128, b.child = a.child, b = uj.bind(null, a), e._reactRetry = b, null;
  a = f.treeContext;
  yg = Lf(e.nextSibling);
  xg = b;
  I2 = true;
  zg = null;
  a !== null && (og[pg++] = rg, og[pg++] = sg, og[pg++] = qg, rg = a.id, sg = a.overflow, qg = b);
  b = qj(b, d.children);
  b.flags |= 4096;
  return b;
}
function vj(a, b, c) {
  a.lanes |= b;
  var d = a.alternate;
  d !== null && (d.lanes |= b);
  bh(a.return, b, c);
}
function wj(a, b, c, d, e) {
  var f = a.memoizedState;
  f === null ? a.memoizedState = { isBackwards: b, rendering: null, renderingStartTime: 0, last: d, tail: c, tailMode: e } : (f.isBackwards = b, f.rendering = null, f.renderingStartTime = 0, f.last = d, f.tail = c, f.tailMode = e);
}
function xj(a, b, c) {
  var d = b.pendingProps, e = d.revealOrder, f = d.tail;
  Xi(a, b, d.children, c);
  d = L2.current;
  if ((d & 2) !== 0)
    d = d & 1 | 2, b.flags |= 128;
  else {
    if (a !== null && (a.flags & 128) !== 0)
      a:
        for (a = b.child;a !== null; ) {
          if (a.tag === 13)
            a.memoizedState !== null && vj(a, c, b);
          else if (a.tag === 19)
            vj(a, c, b);
          else if (a.child !== null) {
            a.child.return = a;
            a = a.child;
            continue;
          }
          if (a === b)
            break a;
          for (;a.sibling === null; ) {
            if (a.return === null || a.return === b)
              break a;
            a = a.return;
          }
          a.sibling.return = a.return;
          a = a.sibling;
        }
    d &= 1;
  }
  G2(L2, d);
  if ((b.mode & 1) === 0)
    b.memoizedState = null;
  else
    switch (e) {
      case "forwards":
        c = b.child;
        for (e = null;c !== null; )
          a = c.alternate, a !== null && Ch(a) === null && (e = c), c = c.sibling;
        c = e;
        c === null ? (e = b.child, b.child = null) : (e = c.sibling, c.sibling = null);
        wj(b, false, e, c, f);
        break;
      case "backwards":
        c = null;
        e = b.child;
        for (b.child = null;e !== null; ) {
          a = e.alternate;
          if (a !== null && Ch(a) === null) {
            b.child = e;
            break;
          }
          a = e.sibling;
          e.sibling = c;
          c = e;
          e = a;
        }
        wj(b, true, c, null, f);
        break;
      case "together":
        wj(b, false, null, null, undefined);
        break;
      default:
        b.memoizedState = null;
    }
  return b.child;
}
function ij(a, b) {
  (b.mode & 1) === 0 && a !== null && (a.alternate = null, b.alternate = null, b.flags |= 2);
}
function Zi(a, b, c) {
  a !== null && (b.dependencies = a.dependencies);
  rh |= b.lanes;
  if ((c & b.childLanes) === 0)
    return null;
  if (a !== null && b.child !== a.child)
    throw Error(p2(153));
  if (b.child !== null) {
    a = b.child;
    c = Pg(a, a.pendingProps);
    b.child = c;
    for (c.return = b;a.sibling !== null; )
      a = a.sibling, c = c.sibling = Pg(a, a.pendingProps), c.return = b;
    c.sibling = null;
  }
  return b.child;
}
function yj(a, b, c) {
  switch (b.tag) {
    case 3:
      kj(b);
      Ig();
      break;
    case 5:
      Ah(b);
      break;
    case 1:
      Zf(b.type) && cg(b);
      break;
    case 4:
      yh(b, b.stateNode.containerInfo);
      break;
    case 10:
      var d = b.type._context, e = b.memoizedProps.value;
      G2(Wg, d._currentValue);
      d._currentValue = e;
      break;
    case 13:
      d = b.memoizedState;
      if (d !== null) {
        if (d.dehydrated !== null)
          return G2(L2, L2.current & 1), b.flags |= 128, null;
        if ((c & b.child.childLanes) !== 0)
          return oj(a, b, c);
        G2(L2, L2.current & 1);
        a = Zi(a, b, c);
        return a !== null ? a.sibling : null;
      }
      G2(L2, L2.current & 1);
      break;
    case 19:
      d = (c & b.childLanes) !== 0;
      if ((a.flags & 128) !== 0) {
        if (d)
          return xj(a, b, c);
        b.flags |= 128;
      }
      e = b.memoizedState;
      e !== null && (e.rendering = null, e.tail = null, e.lastEffect = null);
      G2(L2, L2.current);
      if (d)
        break;
      else
        return null;
    case 22:
    case 23:
      return b.lanes = 0, dj(a, b, c);
  }
  return Zi(a, b, c);
}
function Dj(a, b) {
  if (!I2)
    switch (a.tailMode) {
      case "hidden":
        b = a.tail;
        for (var c = null;b !== null; )
          b.alternate !== null && (c = b), b = b.sibling;
        c === null ? a.tail = null : c.sibling = null;
        break;
      case "collapsed":
        c = a.tail;
        for (var d = null;c !== null; )
          c.alternate !== null && (d = c), c = c.sibling;
        d === null ? b || a.tail === null ? a.tail = null : a.tail.sibling = null : d.sibling = null;
    }
}
function S2(a) {
  var b = a.alternate !== null && a.alternate.child === a.child, c = 0, d = 0;
  if (b)
    for (var e = a.child;e !== null; )
      c |= e.lanes | e.childLanes, d |= e.subtreeFlags & 14680064, d |= e.flags & 14680064, e.return = a, e = e.sibling;
  else
    for (e = a.child;e !== null; )
      c |= e.lanes | e.childLanes, d |= e.subtreeFlags, d |= e.flags, e.return = a, e = e.sibling;
  a.subtreeFlags |= d;
  a.childLanes = c;
  return b;
}
function Ej(a, b, c) {
  var d = b.pendingProps;
  wg(b);
  switch (b.tag) {
    case 2:
    case 16:
    case 15:
    case 0:
    case 11:
    case 7:
    case 8:
    case 12:
    case 9:
    case 14:
      return S2(b), null;
    case 1:
      return Zf(b.type) && $f(), S2(b), null;
    case 3:
      d = b.stateNode;
      zh();
      E2(Wf);
      E2(H2);
      Eh();
      d.pendingContext && (d.context = d.pendingContext, d.pendingContext = null);
      if (a === null || a.child === null)
        Gg(b) ? b.flags |= 4 : a === null || a.memoizedState.isDehydrated && (b.flags & 256) === 0 || (b.flags |= 1024, zg !== null && (Fj(zg), zg = null));
      Aj(a, b);
      S2(b);
      return null;
    case 5:
      Bh(b);
      var e = xh(wh.current);
      c = b.type;
      if (a !== null && b.stateNode != null)
        Bj(a, b, c, d, e), a.ref !== b.ref && (b.flags |= 512, b.flags |= 2097152);
      else {
        if (!d) {
          if (b.stateNode === null)
            throw Error(p2(166));
          S2(b);
          return null;
        }
        a = xh(uh.current);
        if (Gg(b)) {
          d = b.stateNode;
          c = b.type;
          var f = b.memoizedProps;
          d[Of] = b;
          d[Pf] = f;
          a = (b.mode & 1) !== 0;
          switch (c) {
            case "dialog":
              D2("cancel", d);
              D2("close", d);
              break;
            case "iframe":
            case "object":
            case "embed":
              D2("load", d);
              break;
            case "video":
            case "audio":
              for (e = 0;e < lf.length; e++)
                D2(lf[e], d);
              break;
            case "source":
              D2("error", d);
              break;
            case "img":
            case "image":
            case "link":
              D2("error", d);
              D2("load", d);
              break;
            case "details":
              D2("toggle", d);
              break;
            case "input":
              Za(d, f);
              D2("invalid", d);
              break;
            case "select":
              d._wrapperState = { wasMultiple: !!f.multiple };
              D2("invalid", d);
              break;
            case "textarea":
              hb(d, f), D2("invalid", d);
          }
          ub(c, f);
          e = null;
          for (var g in f)
            if (f.hasOwnProperty(g)) {
              var h = f[g];
              g === "children" ? typeof h === "string" ? d.textContent !== h && (f.suppressHydrationWarning !== true && Af(d.textContent, h, a), e = ["children", h]) : typeof h === "number" && d.textContent !== "" + h && (f.suppressHydrationWarning !== true && Af(d.textContent, h, a), e = ["children", "" + h]) : ea.hasOwnProperty(g) && h != null && g === "onScroll" && D2("scroll", d);
            }
          switch (c) {
            case "input":
              Va(d);
              db(d, f, true);
              break;
            case "textarea":
              Va(d);
              jb(d);
              break;
            case "select":
            case "option":
              break;
            default:
              typeof f.onClick === "function" && (d.onclick = Bf);
          }
          d = e;
          b.updateQueue = d;
          d !== null && (b.flags |= 4);
        } else {
          g = e.nodeType === 9 ? e : e.ownerDocument;
          a === "http://www.w3.org/1999/xhtml" && (a = kb(c));
          a === "http://www.w3.org/1999/xhtml" ? c === "script" ? (a = g.createElement("div"), a.innerHTML = "<script></script>", a = a.removeChild(a.firstChild)) : typeof d.is === "string" ? a = g.createElement(c, { is: d.is }) : (a = g.createElement(c), c === "select" && (g = a, d.multiple ? g.multiple = true : d.size && (g.size = d.size))) : a = g.createElementNS(a, c);
          a[Of] = b;
          a[Pf] = d;
          zj(a, b, false, false);
          b.stateNode = a;
          a: {
            g = vb(c, d);
            switch (c) {
              case "dialog":
                D2("cancel", a);
                D2("close", a);
                e = d;
                break;
              case "iframe":
              case "object":
              case "embed":
                D2("load", a);
                e = d;
                break;
              case "video":
              case "audio":
                for (e = 0;e < lf.length; e++)
                  D2(lf[e], a);
                e = d;
                break;
              case "source":
                D2("error", a);
                e = d;
                break;
              case "img":
              case "image":
              case "link":
                D2("error", a);
                D2("load", a);
                e = d;
                break;
              case "details":
                D2("toggle", a);
                e = d;
                break;
              case "input":
                Za(a, d);
                e = Ya(a, d);
                D2("invalid", a);
                break;
              case "option":
                e = d;
                break;
              case "select":
                a._wrapperState = { wasMultiple: !!d.multiple };
                e = A2({}, d, { value: undefined });
                D2("invalid", a);
                break;
              case "textarea":
                hb(a, d);
                e = gb(a, d);
                D2("invalid", a);
                break;
              default:
                e = d;
            }
            ub(c, e);
            h = e;
            for (f in h)
              if (h.hasOwnProperty(f)) {
                var k = h[f];
                f === "style" ? sb(a, k) : f === "dangerouslySetInnerHTML" ? (k = k ? k.__html : undefined, k != null && nb(a, k)) : f === "children" ? typeof k === "string" ? (c !== "textarea" || k !== "") && ob(a, k) : typeof k === "number" && ob(a, "" + k) : f !== "suppressContentEditableWarning" && f !== "suppressHydrationWarning" && f !== "autoFocus" && (ea.hasOwnProperty(f) ? k != null && f === "onScroll" && D2("scroll", a) : k != null && ta(a, f, k, g));
              }
            switch (c) {
              case "input":
                Va(a);
                db(a, d, false);
                break;
              case "textarea":
                Va(a);
                jb(a);
                break;
              case "option":
                d.value != null && a.setAttribute("value", "" + Sa(d.value));
                break;
              case "select":
                a.multiple = !!d.multiple;
                f = d.value;
                f != null ? fb(a, !!d.multiple, f, false) : d.defaultValue != null && fb(a, !!d.multiple, d.defaultValue, true);
                break;
              default:
                typeof e.onClick === "function" && (a.onclick = Bf);
            }
            switch (c) {
              case "button":
              case "input":
              case "select":
              case "textarea":
                d = !!d.autoFocus;
                break a;
              case "img":
                d = true;
                break a;
              default:
                d = false;
            }
          }
          d && (b.flags |= 4);
        }
        b.ref !== null && (b.flags |= 512, b.flags |= 2097152);
      }
      S2(b);
      return null;
    case 6:
      if (a && b.stateNode != null)
        Cj(a, b, a.memoizedProps, d);
      else {
        if (typeof d !== "string" && b.stateNode === null)
          throw Error(p2(166));
        c = xh(wh.current);
        xh(uh.current);
        if (Gg(b)) {
          d = b.stateNode;
          c = b.memoizedProps;
          d[Of] = b;
          if (f = d.nodeValue !== c) {
            if (a = xg, a !== null)
              switch (a.tag) {
                case 3:
                  Af(d.nodeValue, c, (a.mode & 1) !== 0);
                  break;
                case 5:
                  a.memoizedProps.suppressHydrationWarning !== true && Af(d.nodeValue, c, (a.mode & 1) !== 0);
              }
          }
          f && (b.flags |= 4);
        } else
          d = (c.nodeType === 9 ? c : c.ownerDocument).createTextNode(d), d[Of] = b, b.stateNode = d;
      }
      S2(b);
      return null;
    case 13:
      E2(L2);
      d = b.memoizedState;
      if (a === null || a.memoizedState !== null && a.memoizedState.dehydrated !== null) {
        if (I2 && yg !== null && (b.mode & 1) !== 0 && (b.flags & 128) === 0)
          Hg(), Ig(), b.flags |= 98560, f = false;
        else if (f = Gg(b), d !== null && d.dehydrated !== null) {
          if (a === null) {
            if (!f)
              throw Error(p2(318));
            f = b.memoizedState;
            f = f !== null ? f.dehydrated : null;
            if (!f)
              throw Error(p2(317));
            f[Of] = b;
          } else
            Ig(), (b.flags & 128) === 0 && (b.memoizedState = null), b.flags |= 4;
          S2(b);
          f = false;
        } else
          zg !== null && (Fj(zg), zg = null), f = true;
        if (!f)
          return b.flags & 65536 ? b : null;
      }
      if ((b.flags & 128) !== 0)
        return b.lanes = c, b;
      d = d !== null;
      d !== (a !== null && a.memoizedState !== null) && d && (b.child.flags |= 8192, (b.mode & 1) !== 0 && (a === null || (L2.current & 1) !== 0 ? T2 === 0 && (T2 = 3) : tj()));
      b.updateQueue !== null && (b.flags |= 4);
      S2(b);
      return null;
    case 4:
      return zh(), Aj(a, b), a === null && sf(b.stateNode.containerInfo), S2(b), null;
    case 10:
      return ah(b.type._context), S2(b), null;
    case 17:
      return Zf(b.type) && $f(), S2(b), null;
    case 19:
      E2(L2);
      f = b.memoizedState;
      if (f === null)
        return S2(b), null;
      d = (b.flags & 128) !== 0;
      g = f.rendering;
      if (g === null)
        if (d)
          Dj(f, false);
        else {
          if (T2 !== 0 || a !== null && (a.flags & 128) !== 0)
            for (a = b.child;a !== null; ) {
              g = Ch(a);
              if (g !== null) {
                b.flags |= 128;
                Dj(f, false);
                d = g.updateQueue;
                d !== null && (b.updateQueue = d, b.flags |= 4);
                b.subtreeFlags = 0;
                d = c;
                for (c = b.child;c !== null; )
                  f = c, a = d, f.flags &= 14680066, g = f.alternate, g === null ? (f.childLanes = 0, f.lanes = a, f.child = null, f.subtreeFlags = 0, f.memoizedProps = null, f.memoizedState = null, f.updateQueue = null, f.dependencies = null, f.stateNode = null) : (f.childLanes = g.childLanes, f.lanes = g.lanes, f.child = g.child, f.subtreeFlags = 0, f.deletions = null, f.memoizedProps = g.memoizedProps, f.memoizedState = g.memoizedState, f.updateQueue = g.updateQueue, f.type = g.type, a = g.dependencies, f.dependencies = a === null ? null : { lanes: a.lanes, firstContext: a.firstContext }), c = c.sibling;
                G2(L2, L2.current & 1 | 2);
                return b.child;
              }
              a = a.sibling;
            }
          f.tail !== null && B2() > Gj && (b.flags |= 128, d = true, Dj(f, false), b.lanes = 4194304);
        }
      else {
        if (!d)
          if (a = Ch(g), a !== null) {
            if (b.flags |= 128, d = true, c = a.updateQueue, c !== null && (b.updateQueue = c, b.flags |= 4), Dj(f, true), f.tail === null && f.tailMode === "hidden" && !g.alternate && !I2)
              return S2(b), null;
          } else
            2 * B2() - f.renderingStartTime > Gj && c !== 1073741824 && (b.flags |= 128, d = true, Dj(f, false), b.lanes = 4194304);
        f.isBackwards ? (g.sibling = b.child, b.child = g) : (c = f.last, c !== null ? c.sibling = g : b.child = g, f.last = g);
      }
      if (f.tail !== null)
        return b = f.tail, f.rendering = b, f.tail = b.sibling, f.renderingStartTime = B2(), b.sibling = null, c = L2.current, G2(L2, d ? c & 1 | 2 : c & 1), b;
      S2(b);
      return null;
    case 22:
    case 23:
      return Hj(), d = b.memoizedState !== null, a !== null && a.memoizedState !== null !== d && (b.flags |= 8192), d && (b.mode & 1) !== 0 ? (fj & 1073741824) !== 0 && (S2(b), b.subtreeFlags & 6 && (b.flags |= 8192)) : S2(b), null;
    case 24:
      return null;
    case 25:
      return null;
  }
  throw Error(p2(156, b.tag));
}
function Ij(a, b) {
  wg(b);
  switch (b.tag) {
    case 1:
      return Zf(b.type) && $f(), a = b.flags, a & 65536 ? (b.flags = a & -65537 | 128, b) : null;
    case 3:
      return zh(), E2(Wf), E2(H2), Eh(), a = b.flags, (a & 65536) !== 0 && (a & 128) === 0 ? (b.flags = a & -65537 | 128, b) : null;
    case 5:
      return Bh(b), null;
    case 13:
      E2(L2);
      a = b.memoizedState;
      if (a !== null && a.dehydrated !== null) {
        if (b.alternate === null)
          throw Error(p2(340));
        Ig();
      }
      a = b.flags;
      return a & 65536 ? (b.flags = a & -65537 | 128, b) : null;
    case 19:
      return E2(L2), null;
    case 4:
      return zh(), null;
    case 10:
      return ah(b.type._context), null;
    case 22:
    case 23:
      return Hj(), null;
    case 24:
      return null;
    default:
      return null;
  }
}
function Lj(a, b) {
  var c = a.ref;
  if (c !== null)
    if (typeof c === "function")
      try {
        c(null);
      } catch (d) {
        W2(a, b, d);
      }
    else
      c.current = null;
}
function Mj(a, b, c) {
  try {
    c();
  } catch (d) {
    W2(a, b, d);
  }
}
function Oj(a, b) {
  Cf = dd;
  a = Me();
  if (Ne(a)) {
    if ("selectionStart" in a)
      var c = { start: a.selectionStart, end: a.selectionEnd };
    else
      a: {
        c = (c = a.ownerDocument) && c.defaultView || window;
        var d = c.getSelection && c.getSelection();
        if (d && d.rangeCount !== 0) {
          c = d.anchorNode;
          var { anchorOffset: e, focusNode: f } = d;
          d = d.focusOffset;
          try {
            c.nodeType, f.nodeType;
          } catch (F2) {
            c = null;
            break a;
          }
          var g = 0, h = -1, k = -1, l2 = 0, m = 0, q2 = a, r2 = null;
          b:
            for (;; ) {
              for (var y2;; ) {
                q2 !== c || e !== 0 && q2.nodeType !== 3 || (h = g + e);
                q2 !== f || d !== 0 && q2.nodeType !== 3 || (k = g + d);
                q2.nodeType === 3 && (g += q2.nodeValue.length);
                if ((y2 = q2.firstChild) === null)
                  break;
                r2 = q2;
                q2 = y2;
              }
              for (;; ) {
                if (q2 === a)
                  break b;
                r2 === c && ++l2 === e && (h = g);
                r2 === f && ++m === d && (k = g);
                if ((y2 = q2.nextSibling) !== null)
                  break;
                q2 = r2;
                r2 = q2.parentNode;
              }
              q2 = y2;
            }
          c = h === -1 || k === -1 ? null : { start: h, end: k };
        } else
          c = null;
      }
    c = c || { start: 0, end: 0 };
  } else
    c = null;
  Df = { focusedElem: a, selectionRange: c };
  dd = false;
  for (V2 = b;V2 !== null; )
    if (b = V2, a = b.child, (b.subtreeFlags & 1028) !== 0 && a !== null)
      a.return = b, V2 = a;
    else
      for (;V2 !== null; ) {
        b = V2;
        try {
          var n2 = b.alternate;
          if ((b.flags & 1024) !== 0)
            switch (b.tag) {
              case 0:
              case 11:
              case 15:
                break;
              case 1:
                if (n2 !== null) {
                  var { memoizedProps: t2, memoizedState: J2 } = n2, x2 = b.stateNode, w2 = x2.getSnapshotBeforeUpdate(b.elementType === b.type ? t2 : Ci(b.type, t2), J2);
                  x2.__reactInternalSnapshotBeforeUpdate = w2;
                }
                break;
              case 3:
                var u2 = b.stateNode.containerInfo;
                u2.nodeType === 1 ? u2.textContent = "" : u2.nodeType === 9 && u2.documentElement && u2.removeChild(u2.documentElement);
                break;
              case 5:
              case 6:
              case 4:
              case 17:
                break;
              default:
                throw Error(p2(163));
            }
        } catch (F2) {
          W2(b, b.return, F2);
        }
        a = b.sibling;
        if (a !== null) {
          a.return = b.return;
          V2 = a;
          break;
        }
        V2 = b.return;
      }
  n2 = Nj;
  Nj = false;
  return n2;
}
function Pj(a, b, c) {
  var d = b.updateQueue;
  d = d !== null ? d.lastEffect : null;
  if (d !== null) {
    var e = d = d.next;
    do {
      if ((e.tag & a) === a) {
        var f = e.destroy;
        e.destroy = undefined;
        f !== undefined && Mj(b, c, f);
      }
      e = e.next;
    } while (e !== d);
  }
}
function Qj(a, b) {
  b = b.updateQueue;
  b = b !== null ? b.lastEffect : null;
  if (b !== null) {
    var c = b = b.next;
    do {
      if ((c.tag & a) === a) {
        var d = c.create;
        c.destroy = d();
      }
      c = c.next;
    } while (c !== b);
  }
}
function Rj(a) {
  var b = a.ref;
  if (b !== null) {
    var c = a.stateNode;
    switch (a.tag) {
      case 5:
        a = c;
        break;
      default:
        a = c;
    }
    typeof b === "function" ? b(a) : b.current = a;
  }
}
function Sj(a) {
  var b = a.alternate;
  b !== null && (a.alternate = null, Sj(b));
  a.child = null;
  a.deletions = null;
  a.sibling = null;
  a.tag === 5 && (b = a.stateNode, b !== null && (delete b[Of], delete b[Pf], delete b[of], delete b[Qf], delete b[Rf]));
  a.stateNode = null;
  a.return = null;
  a.dependencies = null;
  a.memoizedProps = null;
  a.memoizedState = null;
  a.pendingProps = null;
  a.stateNode = null;
  a.updateQueue = null;
}
function Tj(a) {
  return a.tag === 5 || a.tag === 3 || a.tag === 4;
}
function Uj(a) {
  a:
    for (;; ) {
      for (;a.sibling === null; ) {
        if (a.return === null || Tj(a.return))
          return null;
        a = a.return;
      }
      a.sibling.return = a.return;
      for (a = a.sibling;a.tag !== 5 && a.tag !== 6 && a.tag !== 18; ) {
        if (a.flags & 2)
          continue a;
        if (a.child === null || a.tag === 4)
          continue a;
        else
          a.child.return = a, a = a.child;
      }
      if (!(a.flags & 2))
        return a.stateNode;
    }
}
function Vj(a, b, c) {
  var d = a.tag;
  if (d === 5 || d === 6)
    a = a.stateNode, b ? c.nodeType === 8 ? c.parentNode.insertBefore(a, b) : c.insertBefore(a, b) : (c.nodeType === 8 ? (b = c.parentNode, b.insertBefore(a, c)) : (b = c, b.appendChild(a)), c = c._reactRootContainer, c !== null && c !== undefined || b.onclick !== null || (b.onclick = Bf));
  else if (d !== 4 && (a = a.child, a !== null))
    for (Vj(a, b, c), a = a.sibling;a !== null; )
      Vj(a, b, c), a = a.sibling;
}
function Wj(a, b, c) {
  var d = a.tag;
  if (d === 5 || d === 6)
    a = a.stateNode, b ? c.insertBefore(a, b) : c.appendChild(a);
  else if (d !== 4 && (a = a.child, a !== null))
    for (Wj(a, b, c), a = a.sibling;a !== null; )
      Wj(a, b, c), a = a.sibling;
}
function Yj(a, b, c) {
  for (c = c.child;c !== null; )
    Zj(a, b, c), c = c.sibling;
}
function Zj(a, b, c) {
  if (lc && typeof lc.onCommitFiberUnmount === "function")
    try {
      lc.onCommitFiberUnmount(kc, c);
    } catch (h) {}
  switch (c.tag) {
    case 5:
      U2 || Lj(c, b);
    case 6:
      var d = X2, e = Xj;
      X2 = null;
      Yj(a, b, c);
      X2 = d;
      Xj = e;
      X2 !== null && (Xj ? (a = X2, c = c.stateNode, a.nodeType === 8 ? a.parentNode.removeChild(c) : a.removeChild(c)) : X2.removeChild(c.stateNode));
      break;
    case 18:
      X2 !== null && (Xj ? (a = X2, c = c.stateNode, a.nodeType === 8 ? Kf(a.parentNode, c) : a.nodeType === 1 && Kf(a, c), bd(a)) : Kf(X2, c.stateNode));
      break;
    case 4:
      d = X2;
      e = Xj;
      X2 = c.stateNode.containerInfo;
      Xj = true;
      Yj(a, b, c);
      X2 = d;
      Xj = e;
      break;
    case 0:
    case 11:
    case 14:
    case 15:
      if (!U2 && (d = c.updateQueue, d !== null && (d = d.lastEffect, d !== null))) {
        e = d = d.next;
        do {
          var f = e, g = f.destroy;
          f = f.tag;
          g !== undefined && ((f & 2) !== 0 ? Mj(c, b, g) : (f & 4) !== 0 && Mj(c, b, g));
          e = e.next;
        } while (e !== d);
      }
      Yj(a, b, c);
      break;
    case 1:
      if (!U2 && (Lj(c, b), d = c.stateNode, typeof d.componentWillUnmount === "function"))
        try {
          d.props = c.memoizedProps, d.state = c.memoizedState, d.componentWillUnmount();
        } catch (h) {
          W2(c, b, h);
        }
      Yj(a, b, c);
      break;
    case 21:
      Yj(a, b, c);
      break;
    case 22:
      c.mode & 1 ? (U2 = (d = U2) || c.memoizedState !== null, Yj(a, b, c), U2 = d) : Yj(a, b, c);
      break;
    default:
      Yj(a, b, c);
  }
}
function ak(a) {
  var b = a.updateQueue;
  if (b !== null) {
    a.updateQueue = null;
    var c = a.stateNode;
    c === null && (c = a.stateNode = new Kj);
    b.forEach(function(b2) {
      var d = bk.bind(null, a, b2);
      c.has(b2) || (c.add(b2), b2.then(d, d));
    });
  }
}
function ck(a, b) {
  var c = b.deletions;
  if (c !== null)
    for (var d = 0;d < c.length; d++) {
      var e = c[d];
      try {
        var f = a, g = b, h = g;
        a:
          for (;h !== null; ) {
            switch (h.tag) {
              case 5:
                X2 = h.stateNode;
                Xj = false;
                break a;
              case 3:
                X2 = h.stateNode.containerInfo;
                Xj = true;
                break a;
              case 4:
                X2 = h.stateNode.containerInfo;
                Xj = true;
                break a;
            }
            h = h.return;
          }
        if (X2 === null)
          throw Error(p2(160));
        Zj(f, g, e);
        X2 = null;
        Xj = false;
        var k = e.alternate;
        k !== null && (k.return = null);
        e.return = null;
      } catch (l2) {
        W2(e, b, l2);
      }
    }
  if (b.subtreeFlags & 12854)
    for (b = b.child;b !== null; )
      dk(b, a), b = b.sibling;
}
function dk(a, b) {
  var { alternate: c, flags: d } = a;
  switch (a.tag) {
    case 0:
    case 11:
    case 14:
    case 15:
      ck(b, a);
      ek(a);
      if (d & 4) {
        try {
          Pj(3, a, a.return), Qj(3, a);
        } catch (t2) {
          W2(a, a.return, t2);
        }
        try {
          Pj(5, a, a.return);
        } catch (t2) {
          W2(a, a.return, t2);
        }
      }
      break;
    case 1:
      ck(b, a);
      ek(a);
      d & 512 && c !== null && Lj(c, c.return);
      break;
    case 5:
      ck(b, a);
      ek(a);
      d & 512 && c !== null && Lj(c, c.return);
      if (a.flags & 32) {
        var e = a.stateNode;
        try {
          ob(e, "");
        } catch (t2) {
          W2(a, a.return, t2);
        }
      }
      if (d & 4 && (e = a.stateNode, e != null)) {
        var f = a.memoizedProps, g = c !== null ? c.memoizedProps : f, h = a.type, k = a.updateQueue;
        a.updateQueue = null;
        if (k !== null)
          try {
            h === "input" && f.type === "radio" && f.name != null && ab(e, f);
            vb(h, g);
            var l2 = vb(h, f);
            for (g = 0;g < k.length; g += 2) {
              var m = k[g], q2 = k[g + 1];
              m === "style" ? sb(e, q2) : m === "dangerouslySetInnerHTML" ? nb(e, q2) : m === "children" ? ob(e, q2) : ta(e, m, q2, l2);
            }
            switch (h) {
              case "input":
                bb(e, f);
                break;
              case "textarea":
                ib(e, f);
                break;
              case "select":
                var r2 = e._wrapperState.wasMultiple;
                e._wrapperState.wasMultiple = !!f.multiple;
                var y2 = f.value;
                y2 != null ? fb(e, !!f.multiple, y2, false) : r2 !== !!f.multiple && (f.defaultValue != null ? fb(e, !!f.multiple, f.defaultValue, true) : fb(e, !!f.multiple, f.multiple ? [] : "", false));
            }
            e[Pf] = f;
          } catch (t2) {
            W2(a, a.return, t2);
          }
      }
      break;
    case 6:
      ck(b, a);
      ek(a);
      if (d & 4) {
        if (a.stateNode === null)
          throw Error(p2(162));
        e = a.stateNode;
        f = a.memoizedProps;
        try {
          e.nodeValue = f;
        } catch (t2) {
          W2(a, a.return, t2);
        }
      }
      break;
    case 3:
      ck(b, a);
      ek(a);
      if (d & 4 && c !== null && c.memoizedState.isDehydrated)
        try {
          bd(b.containerInfo);
        } catch (t2) {
          W2(a, a.return, t2);
        }
      break;
    case 4:
      ck(b, a);
      ek(a);
      break;
    case 13:
      ck(b, a);
      ek(a);
      e = a.child;
      e.flags & 8192 && (f = e.memoizedState !== null, e.stateNode.isHidden = f, !f || e.alternate !== null && e.alternate.memoizedState !== null || (fk = B2()));
      d & 4 && ak(a);
      break;
    case 22:
      m = c !== null && c.memoizedState !== null;
      a.mode & 1 ? (U2 = (l2 = U2) || m, ck(b, a), U2 = l2) : ck(b, a);
      ek(a);
      if (d & 8192) {
        l2 = a.memoizedState !== null;
        if ((a.stateNode.isHidden = l2) && !m && (a.mode & 1) !== 0)
          for (V2 = a, m = a.child;m !== null; ) {
            for (q2 = V2 = m;V2 !== null; ) {
              r2 = V2;
              y2 = r2.child;
              switch (r2.tag) {
                case 0:
                case 11:
                case 14:
                case 15:
                  Pj(4, r2, r2.return);
                  break;
                case 1:
                  Lj(r2, r2.return);
                  var n2 = r2.stateNode;
                  if (typeof n2.componentWillUnmount === "function") {
                    d = r2;
                    c = r2.return;
                    try {
                      b = d, n2.props = b.memoizedProps, n2.state = b.memoizedState, n2.componentWillUnmount();
                    } catch (t2) {
                      W2(d, c, t2);
                    }
                  }
                  break;
                case 5:
                  Lj(r2, r2.return);
                  break;
                case 22:
                  if (r2.memoizedState !== null) {
                    gk(q2);
                    continue;
                  }
              }
              y2 !== null ? (y2.return = r2, V2 = y2) : gk(q2);
            }
            m = m.sibling;
          }
        a:
          for (m = null, q2 = a;; ) {
            if (q2.tag === 5) {
              if (m === null) {
                m = q2;
                try {
                  e = q2.stateNode, l2 ? (f = e.style, typeof f.setProperty === "function" ? f.setProperty("display", "none", "important") : f.display = "none") : (h = q2.stateNode, k = q2.memoizedProps.style, g = k !== undefined && k !== null && k.hasOwnProperty("display") ? k.display : null, h.style.display = rb("display", g));
                } catch (t2) {
                  W2(a, a.return, t2);
                }
              }
            } else if (q2.tag === 6) {
              if (m === null)
                try {
                  q2.stateNode.nodeValue = l2 ? "" : q2.memoizedProps;
                } catch (t2) {
                  W2(a, a.return, t2);
                }
            } else if ((q2.tag !== 22 && q2.tag !== 23 || q2.memoizedState === null || q2 === a) && q2.child !== null) {
              q2.child.return = q2;
              q2 = q2.child;
              continue;
            }
            if (q2 === a)
              break a;
            for (;q2.sibling === null; ) {
              if (q2.return === null || q2.return === a)
                break a;
              m === q2 && (m = null);
              q2 = q2.return;
            }
            m === q2 && (m = null);
            q2.sibling.return = q2.return;
            q2 = q2.sibling;
          }
      }
      break;
    case 19:
      ck(b, a);
      ek(a);
      d & 4 && ak(a);
      break;
    case 21:
      break;
    default:
      ck(b, a), ek(a);
  }
}
function ek(a) {
  var b = a.flags;
  if (b & 2) {
    try {
      a: {
        for (var c = a.return;c !== null; ) {
          if (Tj(c)) {
            var d = c;
            break a;
          }
          c = c.return;
        }
        throw Error(p2(160));
      }
      switch (d.tag) {
        case 5:
          var e = d.stateNode;
          d.flags & 32 && (ob(e, ""), d.flags &= -33);
          var f = Uj(a);
          Wj(a, f, e);
          break;
        case 3:
        case 4:
          var g = d.stateNode.containerInfo, h = Uj(a);
          Vj(a, h, g);
          break;
        default:
          throw Error(p2(161));
      }
    } catch (k) {
      W2(a, a.return, k);
    }
    a.flags &= -3;
  }
  b & 4096 && (a.flags &= -4097);
}
function hk(a, b, c) {
  V2 = a;
  ik(a, b, c);
}
function ik(a, b, c) {
  for (var d = (a.mode & 1) !== 0;V2 !== null; ) {
    var e = V2, f = e.child;
    if (e.tag === 22 && d) {
      var g = e.memoizedState !== null || Jj;
      if (!g) {
        var h = e.alternate, k = h !== null && h.memoizedState !== null || U2;
        h = Jj;
        var l2 = U2;
        Jj = g;
        if ((U2 = k) && !l2)
          for (V2 = e;V2 !== null; )
            g = V2, k = g.child, g.tag === 22 && g.memoizedState !== null ? jk(e) : k !== null ? (k.return = g, V2 = k) : jk(e);
        for (;f !== null; )
          V2 = f, ik(f, b, c), f = f.sibling;
        V2 = e;
        Jj = h;
        U2 = l2;
      }
      kk(a, b, c);
    } else
      (e.subtreeFlags & 8772) !== 0 && f !== null ? (f.return = e, V2 = f) : kk(a, b, c);
  }
}
function kk(a) {
  for (;V2 !== null; ) {
    var b = V2;
    if ((b.flags & 8772) !== 0) {
      var c = b.alternate;
      try {
        if ((b.flags & 8772) !== 0)
          switch (b.tag) {
            case 0:
            case 11:
            case 15:
              U2 || Qj(5, b);
              break;
            case 1:
              var d = b.stateNode;
              if (b.flags & 4 && !U2)
                if (c === null)
                  d.componentDidMount();
                else {
                  var e = b.elementType === b.type ? c.memoizedProps : Ci(b.type, c.memoizedProps);
                  d.componentDidUpdate(e, c.memoizedState, d.__reactInternalSnapshotBeforeUpdate);
                }
              var f = b.updateQueue;
              f !== null && sh(b, f, d);
              break;
            case 3:
              var g = b.updateQueue;
              if (g !== null) {
                c = null;
                if (b.child !== null)
                  switch (b.child.tag) {
                    case 5:
                      c = b.child.stateNode;
                      break;
                    case 1:
                      c = b.child.stateNode;
                  }
                sh(b, g, c);
              }
              break;
            case 5:
              var h = b.stateNode;
              if (c === null && b.flags & 4) {
                c = h;
                var k = b.memoizedProps;
                switch (b.type) {
                  case "button":
                  case "input":
                  case "select":
                  case "textarea":
                    k.autoFocus && c.focus();
                    break;
                  case "img":
                    k.src && (c.src = k.src);
                }
              }
              break;
            case 6:
              break;
            case 4:
              break;
            case 12:
              break;
            case 13:
              if (b.memoizedState === null) {
                var l2 = b.alternate;
                if (l2 !== null) {
                  var m = l2.memoizedState;
                  if (m !== null) {
                    var q2 = m.dehydrated;
                    q2 !== null && bd(q2);
                  }
                }
              }
              break;
            case 19:
            case 17:
            case 21:
            case 22:
            case 23:
            case 25:
              break;
            default:
              throw Error(p2(163));
          }
        U2 || b.flags & 512 && Rj(b);
      } catch (r2) {
        W2(b, b.return, r2);
      }
    }
    if (b === a) {
      V2 = null;
      break;
    }
    c = b.sibling;
    if (c !== null) {
      c.return = b.return;
      V2 = c;
      break;
    }
    V2 = b.return;
  }
}
function gk(a) {
  for (;V2 !== null; ) {
    var b = V2;
    if (b === a) {
      V2 = null;
      break;
    }
    var c = b.sibling;
    if (c !== null) {
      c.return = b.return;
      V2 = c;
      break;
    }
    V2 = b.return;
  }
}
function jk(a) {
  for (;V2 !== null; ) {
    var b = V2;
    try {
      switch (b.tag) {
        case 0:
        case 11:
        case 15:
          var c = b.return;
          try {
            Qj(4, b);
          } catch (k) {
            W2(b, c, k);
          }
          break;
        case 1:
          var d = b.stateNode;
          if (typeof d.componentDidMount === "function") {
            var e = b.return;
            try {
              d.componentDidMount();
            } catch (k) {
              W2(b, e, k);
            }
          }
          var f = b.return;
          try {
            Rj(b);
          } catch (k) {
            W2(b, f, k);
          }
          break;
        case 5:
          var g = b.return;
          try {
            Rj(b);
          } catch (k) {
            W2(b, g, k);
          }
      }
    } catch (k) {
      W2(b, b.return, k);
    }
    if (b === a) {
      V2 = null;
      break;
    }
    var h = b.sibling;
    if (h !== null) {
      h.return = b.return;
      V2 = h;
      break;
    }
    V2 = b.return;
  }
}
function R2() {
  return (K2 & 6) !== 0 ? B2() : Ak !== -1 ? Ak : Ak = B2();
}
function yi(a) {
  if ((a.mode & 1) === 0)
    return 1;
  if ((K2 & 2) !== 0 && Z !== 0)
    return Z & -Z;
  if (Kg.transition !== null)
    return Bk === 0 && (Bk = yc()), Bk;
  a = C2;
  if (a !== 0)
    return a;
  a = window.event;
  a = a === undefined ? 16 : jd(a.type);
  return a;
}
function gi(a, b, c, d) {
  if (50 < yk)
    throw yk = 0, zk = null, Error(p2(185));
  Ac(a, c, d);
  if ((K2 & 2) === 0 || a !== Q2)
    a === Q2 && ((K2 & 2) === 0 && (qk |= c), T2 === 4 && Ck(a, Z)), Dk(a, d), c === 1 && K2 === 0 && (b.mode & 1) === 0 && (Gj = B2() + 500, fg && jg());
}
function Dk(a, b) {
  var c = a.callbackNode;
  wc(a, b);
  var d = uc(a, a === Q2 ? Z : 0);
  if (d === 0)
    c !== null && bc(c), a.callbackNode = null, a.callbackPriority = 0;
  else if (b = d & -d, a.callbackPriority !== b) {
    c != null && bc(c);
    if (b === 1)
      a.tag === 0 ? ig(Ek.bind(null, a)) : hg(Ek.bind(null, a)), Jf(function() {
        (K2 & 6) === 0 && jg();
      }), c = null;
    else {
      switch (Dc(d)) {
        case 1:
          c = fc;
          break;
        case 4:
          c = gc;
          break;
        case 16:
          c = hc;
          break;
        case 536870912:
          c = jc;
          break;
        default:
          c = hc;
      }
      c = Fk(c, Gk.bind(null, a));
    }
    a.callbackPriority = b;
    a.callbackNode = c;
  }
}
function Gk(a, b) {
  Ak = -1;
  Bk = 0;
  if ((K2 & 6) !== 0)
    throw Error(p2(327));
  var c = a.callbackNode;
  if (Hk() && a.callbackNode !== c)
    return null;
  var d = uc(a, a === Q2 ? Z : 0);
  if (d === 0)
    return null;
  if ((d & 30) !== 0 || (d & a.expiredLanes) !== 0 || b)
    b = Ik(a, d);
  else {
    b = d;
    var e = K2;
    K2 |= 2;
    var f = Jk();
    if (Q2 !== a || Z !== b)
      uk = null, Gj = B2() + 500, Kk(a, b);
    do
      try {
        Lk();
        break;
      } catch (h) {
        Mk(a, h);
      }
    while (1);
    $g();
    mk.current = f;
    K2 = e;
    Y !== null ? b = 0 : (Q2 = null, Z = 0, b = T2);
  }
  if (b !== 0) {
    b === 2 && (e = xc(a), e !== 0 && (d = e, b = Nk(a, e)));
    if (b === 1)
      throw c = pk, Kk(a, 0), Ck(a, d), Dk(a, B2()), c;
    if (b === 6)
      Ck(a, d);
    else {
      e = a.current.alternate;
      if ((d & 30) === 0 && !Ok(e) && (b = Ik(a, d), b === 2 && (f = xc(a), f !== 0 && (d = f, b = Nk(a, f))), b === 1))
        throw c = pk, Kk(a, 0), Ck(a, d), Dk(a, B2()), c;
      a.finishedWork = e;
      a.finishedLanes = d;
      switch (b) {
        case 0:
        case 1:
          throw Error(p2(345));
        case 2:
          Pk(a, tk, uk);
          break;
        case 3:
          Ck(a, d);
          if ((d & 130023424) === d && (b = fk + 500 - B2(), 10 < b)) {
            if (uc(a, 0) !== 0)
              break;
            e = a.suspendedLanes;
            if ((e & d) !== d) {
              R2();
              a.pingedLanes |= a.suspendedLanes & e;
              break;
            }
            a.timeoutHandle = Ff(Pk.bind(null, a, tk, uk), b);
            break;
          }
          Pk(a, tk, uk);
          break;
        case 4:
          Ck(a, d);
          if ((d & 4194240) === d)
            break;
          b = a.eventTimes;
          for (e = -1;0 < d; ) {
            var g = 31 - oc(d);
            f = 1 << g;
            g = b[g];
            g > e && (e = g);
            d &= ~f;
          }
          d = e;
          d = B2() - d;
          d = (120 > d ? 120 : 480 > d ? 480 : 1080 > d ? 1080 : 1920 > d ? 1920 : 3000 > d ? 3000 : 4320 > d ? 4320 : 1960 * lk(d / 1960)) - d;
          if (10 < d) {
            a.timeoutHandle = Ff(Pk.bind(null, a, tk, uk), d);
            break;
          }
          Pk(a, tk, uk);
          break;
        case 5:
          Pk(a, tk, uk);
          break;
        default:
          throw Error(p2(329));
      }
    }
  }
  Dk(a, B2());
  return a.callbackNode === c ? Gk.bind(null, a) : null;
}
function Nk(a, b) {
  var c = sk;
  a.current.memoizedState.isDehydrated && (Kk(a, b).flags |= 256);
  a = Ik(a, b);
  a !== 2 && (b = tk, tk = c, b !== null && Fj(b));
  return a;
}
function Fj(a) {
  tk === null ? tk = a : tk.push.apply(tk, a);
}
function Ok(a) {
  for (var b = a;; ) {
    if (b.flags & 16384) {
      var c = b.updateQueue;
      if (c !== null && (c = c.stores, c !== null))
        for (var d = 0;d < c.length; d++) {
          var e = c[d], f = e.getSnapshot;
          e = e.value;
          try {
            if (!He(f(), e))
              return false;
          } catch (g) {
            return false;
          }
        }
    }
    c = b.child;
    if (b.subtreeFlags & 16384 && c !== null)
      c.return = b, b = c;
    else {
      if (b === a)
        break;
      for (;b.sibling === null; ) {
        if (b.return === null || b.return === a)
          return true;
        b = b.return;
      }
      b.sibling.return = b.return;
      b = b.sibling;
    }
  }
  return true;
}
function Ck(a, b) {
  b &= ~rk;
  b &= ~qk;
  a.suspendedLanes |= b;
  a.pingedLanes &= ~b;
  for (a = a.expirationTimes;0 < b; ) {
    var c = 31 - oc(b), d = 1 << c;
    a[c] = -1;
    b &= ~d;
  }
}
function Ek(a) {
  if ((K2 & 6) !== 0)
    throw Error(p2(327));
  Hk();
  var b = uc(a, 0);
  if ((b & 1) === 0)
    return Dk(a, B2()), null;
  var c = Ik(a, b);
  if (a.tag !== 0 && c === 2) {
    var d = xc(a);
    d !== 0 && (b = d, c = Nk(a, d));
  }
  if (c === 1)
    throw c = pk, Kk(a, 0), Ck(a, b), Dk(a, B2()), c;
  if (c === 6)
    throw Error(p2(345));
  a.finishedWork = a.current.alternate;
  a.finishedLanes = b;
  Pk(a, tk, uk);
  Dk(a, B2());
  return null;
}
function Qk(a, b) {
  var c = K2;
  K2 |= 1;
  try {
    return a(b);
  } finally {
    K2 = c, K2 === 0 && (Gj = B2() + 500, fg && jg());
  }
}
function Rk(a) {
  wk !== null && wk.tag === 0 && (K2 & 6) === 0 && Hk();
  var b = K2;
  K2 |= 1;
  var c = ok.transition, d = C2;
  try {
    if (ok.transition = null, C2 = 1, a)
      return a();
  } finally {
    C2 = d, ok.transition = c, K2 = b, (K2 & 6) === 0 && jg();
  }
}
function Hj() {
  fj = ej.current;
  E2(ej);
}
function Kk(a, b) {
  a.finishedWork = null;
  a.finishedLanes = 0;
  var c = a.timeoutHandle;
  c !== -1 && (a.timeoutHandle = -1, Gf(c));
  if (Y !== null)
    for (c = Y.return;c !== null; ) {
      var d = c;
      wg(d);
      switch (d.tag) {
        case 1:
          d = d.type.childContextTypes;
          d !== null && d !== undefined && $f();
          break;
        case 3:
          zh();
          E2(Wf);
          E2(H2);
          Eh();
          break;
        case 5:
          Bh(d);
          break;
        case 4:
          zh();
          break;
        case 13:
          E2(L2);
          break;
        case 19:
          E2(L2);
          break;
        case 10:
          ah(d.type._context);
          break;
        case 22:
        case 23:
          Hj();
      }
      c = c.return;
    }
  Q2 = a;
  Y = a = Pg(a.current, null);
  Z = fj = b;
  T2 = 0;
  pk = null;
  rk = qk = rh = 0;
  tk = sk = null;
  if (fh !== null) {
    for (b = 0;b < fh.length; b++)
      if (c = fh[b], d = c.interleaved, d !== null) {
        c.interleaved = null;
        var e = d.next, f = c.pending;
        if (f !== null) {
          var g = f.next;
          f.next = e;
          d.next = g;
        }
        c.pending = d;
      }
    fh = null;
  }
  return a;
}
function Mk(a, b) {
  do {
    var c = Y;
    try {
      $g();
      Fh.current = Rh;
      if (Ih) {
        for (var d = M2.memoizedState;d !== null; ) {
          var e = d.queue;
          e !== null && (e.pending = null);
          d = d.next;
        }
        Ih = false;
      }
      Hh = 0;
      O2 = N2 = M2 = null;
      Jh = false;
      Kh = 0;
      nk.current = null;
      if (c === null || c.return === null) {
        T2 = 1;
        pk = b;
        Y = null;
        break;
      }
      a: {
        var f = a, g = c.return, h = c, k = b;
        b = Z;
        h.flags |= 32768;
        if (k !== null && typeof k === "object" && typeof k.then === "function") {
          var l2 = k, m = h, q2 = m.tag;
          if ((m.mode & 1) === 0 && (q2 === 0 || q2 === 11 || q2 === 15)) {
            var r2 = m.alternate;
            r2 ? (m.updateQueue = r2.updateQueue, m.memoizedState = r2.memoizedState, m.lanes = r2.lanes) : (m.updateQueue = null, m.memoizedState = null);
          }
          var y2 = Ui(g);
          if (y2 !== null) {
            y2.flags &= -257;
            Vi(y2, g, h, f, b);
            y2.mode & 1 && Si(f, l2, b);
            b = y2;
            k = l2;
            var n2 = b.updateQueue;
            if (n2 === null) {
              var t2 = new Set;
              t2.add(k);
              b.updateQueue = t2;
            } else
              n2.add(k);
            break a;
          } else {
            if ((b & 1) === 0) {
              Si(f, l2, b);
              tj();
              break a;
            }
            k = Error(p2(426));
          }
        } else if (I2 && h.mode & 1) {
          var J2 = Ui(g);
          if (J2 !== null) {
            (J2.flags & 65536) === 0 && (J2.flags |= 256);
            Vi(J2, g, h, f, b);
            Jg(Ji(k, h));
            break a;
          }
        }
        f = k = Ji(k, h);
        T2 !== 4 && (T2 = 2);
        sk === null ? sk = [f] : sk.push(f);
        f = g;
        do {
          switch (f.tag) {
            case 3:
              f.flags |= 65536;
              b &= -b;
              f.lanes |= b;
              var x2 = Ni(f, k, b);
              ph(f, x2);
              break a;
            case 1:
              h = k;
              var { type: w2, stateNode: u2 } = f;
              if ((f.flags & 128) === 0 && (typeof w2.getDerivedStateFromError === "function" || u2 !== null && typeof u2.componentDidCatch === "function" && (Ri === null || !Ri.has(u2)))) {
                f.flags |= 65536;
                b &= -b;
                f.lanes |= b;
                var F2 = Qi(f, h, b);
                ph(f, F2);
                break a;
              }
          }
          f = f.return;
        } while (f !== null);
      }
      Sk(c);
    } catch (na) {
      b = na;
      Y === c && c !== null && (Y = c = c.return);
      continue;
    }
    break;
  } while (1);
}
function Jk() {
  var a = mk.current;
  mk.current = Rh;
  return a === null ? Rh : a;
}
function tj() {
  if (T2 === 0 || T2 === 3 || T2 === 2)
    T2 = 4;
  Q2 === null || (rh & 268435455) === 0 && (qk & 268435455) === 0 || Ck(Q2, Z);
}
function Ik(a, b) {
  var c = K2;
  K2 |= 2;
  var d = Jk();
  if (Q2 !== a || Z !== b)
    uk = null, Kk(a, b);
  do
    try {
      Tk();
      break;
    } catch (e) {
      Mk(a, e);
    }
  while (1);
  $g();
  K2 = c;
  mk.current = d;
  if (Y !== null)
    throw Error(p2(261));
  Q2 = null;
  Z = 0;
  return T2;
}
function Tk() {
  for (;Y !== null; )
    Uk(Y);
}
function Lk() {
  for (;Y !== null && !cc(); )
    Uk(Y);
}
function Uk(a) {
  var b = Vk(a.alternate, a, fj);
  a.memoizedProps = a.pendingProps;
  b === null ? Sk(a) : Y = b;
  nk.current = null;
}
function Sk(a) {
  var b = a;
  do {
    var c = b.alternate;
    a = b.return;
    if ((b.flags & 32768) === 0) {
      if (c = Ej(c, b, fj), c !== null) {
        Y = c;
        return;
      }
    } else {
      c = Ij(c, b);
      if (c !== null) {
        c.flags &= 32767;
        Y = c;
        return;
      }
      if (a !== null)
        a.flags |= 32768, a.subtreeFlags = 0, a.deletions = null;
      else {
        T2 = 6;
        Y = null;
        return;
      }
    }
    b = b.sibling;
    if (b !== null) {
      Y = b;
      return;
    }
    Y = b = a;
  } while (b !== null);
  T2 === 0 && (T2 = 5);
}
function Pk(a, b, c) {
  var d = C2, e = ok.transition;
  try {
    ok.transition = null, C2 = 1, Wk(a, b, c, d);
  } finally {
    ok.transition = e, C2 = d;
  }
  return null;
}
function Wk(a, b, c, d) {
  do
    Hk();
  while (wk !== null);
  if ((K2 & 6) !== 0)
    throw Error(p2(327));
  c = a.finishedWork;
  var e = a.finishedLanes;
  if (c === null)
    return null;
  a.finishedWork = null;
  a.finishedLanes = 0;
  if (c === a.current)
    throw Error(p2(177));
  a.callbackNode = null;
  a.callbackPriority = 0;
  var f = c.lanes | c.childLanes;
  Bc(a, f);
  a === Q2 && (Y = Q2 = null, Z = 0);
  (c.subtreeFlags & 2064) === 0 && (c.flags & 2064) === 0 || vk || (vk = true, Fk(hc, function() {
    Hk();
    return null;
  }));
  f = (c.flags & 15990) !== 0;
  if ((c.subtreeFlags & 15990) !== 0 || f) {
    f = ok.transition;
    ok.transition = null;
    var g = C2;
    C2 = 1;
    var h = K2;
    K2 |= 4;
    nk.current = null;
    Oj(a, c);
    dk(c, a);
    Oe(Df);
    dd = !!Cf;
    Df = Cf = null;
    a.current = c;
    hk(c, a, e);
    dc();
    K2 = h;
    C2 = g;
    ok.transition = f;
  } else
    a.current = c;
  vk && (vk = false, wk = a, xk = e);
  f = a.pendingLanes;
  f === 0 && (Ri = null);
  mc(c.stateNode, d);
  Dk(a, B2());
  if (b !== null)
    for (d = a.onRecoverableError, c = 0;c < b.length; c++)
      e = b[c], d(e.value, { componentStack: e.stack, digest: e.digest });
  if (Oi)
    throw Oi = false, a = Pi, Pi = null, a;
  (xk & 1) !== 0 && a.tag !== 0 && Hk();
  f = a.pendingLanes;
  (f & 1) !== 0 ? a === zk ? yk++ : (yk = 0, zk = a) : yk = 0;
  jg();
  return null;
}
function Hk() {
  if (wk !== null) {
    var a = Dc(xk), b = ok.transition, c = C2;
    try {
      ok.transition = null;
      C2 = 16 > a ? 16 : a;
      if (wk === null)
        var d = false;
      else {
        a = wk;
        wk = null;
        xk = 0;
        if ((K2 & 6) !== 0)
          throw Error(p2(331));
        var e = K2;
        K2 |= 4;
        for (V2 = a.current;V2 !== null; ) {
          var f = V2, g = f.child;
          if ((V2.flags & 16) !== 0) {
            var h = f.deletions;
            if (h !== null) {
              for (var k = 0;k < h.length; k++) {
                var l2 = h[k];
                for (V2 = l2;V2 !== null; ) {
                  var m = V2;
                  switch (m.tag) {
                    case 0:
                    case 11:
                    case 15:
                      Pj(8, m, f);
                  }
                  var q2 = m.child;
                  if (q2 !== null)
                    q2.return = m, V2 = q2;
                  else
                    for (;V2 !== null; ) {
                      m = V2;
                      var { sibling: r2, return: y2 } = m;
                      Sj(m);
                      if (m === l2) {
                        V2 = null;
                        break;
                      }
                      if (r2 !== null) {
                        r2.return = y2;
                        V2 = r2;
                        break;
                      }
                      V2 = y2;
                    }
                }
              }
              var n2 = f.alternate;
              if (n2 !== null) {
                var t2 = n2.child;
                if (t2 !== null) {
                  n2.child = null;
                  do {
                    var J2 = t2.sibling;
                    t2.sibling = null;
                    t2 = J2;
                  } while (t2 !== null);
                }
              }
              V2 = f;
            }
          }
          if ((f.subtreeFlags & 2064) !== 0 && g !== null)
            g.return = f, V2 = g;
          else
            b:
              for (;V2 !== null; ) {
                f = V2;
                if ((f.flags & 2048) !== 0)
                  switch (f.tag) {
                    case 0:
                    case 11:
                    case 15:
                      Pj(9, f, f.return);
                  }
                var x2 = f.sibling;
                if (x2 !== null) {
                  x2.return = f.return;
                  V2 = x2;
                  break b;
                }
                V2 = f.return;
              }
        }
        var w2 = a.current;
        for (V2 = w2;V2 !== null; ) {
          g = V2;
          var u2 = g.child;
          if ((g.subtreeFlags & 2064) !== 0 && u2 !== null)
            u2.return = g, V2 = u2;
          else
            b:
              for (g = w2;V2 !== null; ) {
                h = V2;
                if ((h.flags & 2048) !== 0)
                  try {
                    switch (h.tag) {
                      case 0:
                      case 11:
                      case 15:
                        Qj(9, h);
                    }
                  } catch (na) {
                    W2(h, h.return, na);
                  }
                if (h === g) {
                  V2 = null;
                  break b;
                }
                var F2 = h.sibling;
                if (F2 !== null) {
                  F2.return = h.return;
                  V2 = F2;
                  break b;
                }
                V2 = h.return;
              }
        }
        K2 = e;
        jg();
        if (lc && typeof lc.onPostCommitFiberRoot === "function")
          try {
            lc.onPostCommitFiberRoot(kc, a);
          } catch (na) {}
        d = true;
      }
      return d;
    } finally {
      C2 = c, ok.transition = b;
    }
  }
  return false;
}
function Xk(a, b, c) {
  b = Ji(c, b);
  b = Ni(a, b, 1);
  a = nh(a, b, 1);
  b = R2();
  a !== null && (Ac(a, 1, b), Dk(a, b));
}
function W2(a, b, c) {
  if (a.tag === 3)
    Xk(a, a, c);
  else
    for (;b !== null; ) {
      if (b.tag === 3) {
        Xk(b, a, c);
        break;
      } else if (b.tag === 1) {
        var d = b.stateNode;
        if (typeof b.type.getDerivedStateFromError === "function" || typeof d.componentDidCatch === "function" && (Ri === null || !Ri.has(d))) {
          a = Ji(c, a);
          a = Qi(b, a, 1);
          b = nh(b, a, 1);
          a = R2();
          b !== null && (Ac(b, 1, a), Dk(b, a));
          break;
        }
      }
      b = b.return;
    }
}
function Ti(a, b, c) {
  var d = a.pingCache;
  d !== null && d.delete(b);
  b = R2();
  a.pingedLanes |= a.suspendedLanes & c;
  Q2 === a && (Z & c) === c && (T2 === 4 || T2 === 3 && (Z & 130023424) === Z && 500 > B2() - fk ? Kk(a, 0) : rk |= c);
  Dk(a, b);
}
function Yk(a, b) {
  b === 0 && ((a.mode & 1) === 0 ? b = 1 : (b = sc, sc <<= 1, (sc & 130023424) === 0 && (sc = 4194304)));
  var c = R2();
  a = ih(a, b);
  a !== null && (Ac(a, b, c), Dk(a, c));
}
function uj(a) {
  var b = a.memoizedState, c = 0;
  b !== null && (c = b.retryLane);
  Yk(a, c);
}
function bk(a, b) {
  var c = 0;
  switch (a.tag) {
    case 13:
      var d = a.stateNode;
      var e = a.memoizedState;
      e !== null && (c = e.retryLane);
      break;
    case 19:
      d = a.stateNode;
      break;
    default:
      throw Error(p2(314));
  }
  d !== null && d.delete(b);
  Yk(a, c);
}
function Fk(a, b) {
  return ac(a, b);
}
function $k(a, b, c, d) {
  this.tag = a;
  this.key = c;
  this.sibling = this.child = this.return = this.stateNode = this.type = this.elementType = null;
  this.index = 0;
  this.ref = null;
  this.pendingProps = b;
  this.dependencies = this.memoizedState = this.updateQueue = this.memoizedProps = null;
  this.mode = d;
  this.subtreeFlags = this.flags = 0;
  this.deletions = null;
  this.childLanes = this.lanes = 0;
  this.alternate = null;
}
function Bg(a, b, c, d) {
  return new $k(a, b, c, d);
}
function aj(a) {
  a = a.prototype;
  return !(!a || !a.isReactComponent);
}
function Zk(a) {
  if (typeof a === "function")
    return aj(a) ? 1 : 0;
  if (a !== undefined && a !== null) {
    a = a.$$typeof;
    if (a === Da)
      return 11;
    if (a === Ga)
      return 14;
  }
  return 2;
}
function Pg(a, b) {
  var c = a.alternate;
  c === null ? (c = Bg(a.tag, b, a.key, a.mode), c.elementType = a.elementType, c.type = a.type, c.stateNode = a.stateNode, c.alternate = a, a.alternate = c) : (c.pendingProps = b, c.type = a.type, c.flags = 0, c.subtreeFlags = 0, c.deletions = null);
  c.flags = a.flags & 14680064;
  c.childLanes = a.childLanes;
  c.lanes = a.lanes;
  c.child = a.child;
  c.memoizedProps = a.memoizedProps;
  c.memoizedState = a.memoizedState;
  c.updateQueue = a.updateQueue;
  b = a.dependencies;
  c.dependencies = b === null ? null : { lanes: b.lanes, firstContext: b.firstContext };
  c.sibling = a.sibling;
  c.index = a.index;
  c.ref = a.ref;
  return c;
}
function Rg(a, b, c, d, e, f) {
  var g = 2;
  d = a;
  if (typeof a === "function")
    aj(a) && (g = 1);
  else if (typeof a === "string")
    g = 5;
  else
    a:
      switch (a) {
        case ya:
          return Tg(c.children, e, f, b);
        case za:
          g = 8;
          e |= 8;
          break;
        case Aa:
          return a = Bg(12, c, b, e | 2), a.elementType = Aa, a.lanes = f, a;
        case Ea:
          return a = Bg(13, c, b, e), a.elementType = Ea, a.lanes = f, a;
        case Fa:
          return a = Bg(19, c, b, e), a.elementType = Fa, a.lanes = f, a;
        case Ia:
          return pj(c, e, f, b);
        default:
          if (typeof a === "object" && a !== null)
            switch (a.$$typeof) {
              case Ba:
                g = 10;
                break a;
              case Ca:
                g = 9;
                break a;
              case Da:
                g = 11;
                break a;
              case Ga:
                g = 14;
                break a;
              case Ha:
                g = 16;
                d = null;
                break a;
            }
          throw Error(p2(130, a == null ? a : typeof a, ""));
      }
  b = Bg(g, c, b, e);
  b.elementType = a;
  b.type = d;
  b.lanes = f;
  return b;
}
function Tg(a, b, c, d) {
  a = Bg(7, a, d, b);
  a.lanes = c;
  return a;
}
function pj(a, b, c, d) {
  a = Bg(22, a, d, b);
  a.elementType = Ia;
  a.lanes = c;
  a.stateNode = { isHidden: false };
  return a;
}
function Qg(a, b, c) {
  a = Bg(6, a, null, b);
  a.lanes = c;
  return a;
}
function Sg(a, b, c) {
  b = Bg(4, a.children !== null ? a.children : [], a.key, b);
  b.lanes = c;
  b.stateNode = { containerInfo: a.containerInfo, pendingChildren: null, implementation: a.implementation };
  return b;
}
function al(a, b, c, d, e) {
  this.tag = b;
  this.containerInfo = a;
  this.finishedWork = this.pingCache = this.current = this.pendingChildren = null;
  this.timeoutHandle = -1;
  this.callbackNode = this.pendingContext = this.context = null;
  this.callbackPriority = 0;
  this.eventTimes = zc(0);
  this.expirationTimes = zc(-1);
  this.entangledLanes = this.finishedLanes = this.mutableReadLanes = this.expiredLanes = this.pingedLanes = this.suspendedLanes = this.pendingLanes = 0;
  this.entanglements = zc(0);
  this.identifierPrefix = d;
  this.onRecoverableError = e;
  this.mutableSourceEagerHydrationData = null;
}
function bl(a, b, c, d, e, f, g, h, k) {
  a = new al(a, b, c, h, k);
  b === 1 ? (b = 1, f === true && (b |= 8)) : b = 0;
  f = Bg(3, null, null, b);
  a.current = f;
  f.stateNode = a;
  f.memoizedState = { element: d, isDehydrated: c, cache: null, transitions: null, pendingSuspenseBoundaries: null };
  kh(f);
  return a;
}
function cl(a, b, c) {
  var d = 3 < arguments.length && arguments[3] !== undefined ? arguments[3] : null;
  return { $$typeof: wa, key: d == null ? null : "" + d, children: a, containerInfo: b, implementation: c };
}
function dl(a) {
  if (!a)
    return Vf;
  a = a._reactInternals;
  a: {
    if (Vb(a) !== a || a.tag !== 1)
      throw Error(p2(170));
    var b = a;
    do {
      switch (b.tag) {
        case 3:
          b = b.stateNode.context;
          break a;
        case 1:
          if (Zf(b.type)) {
            b = b.stateNode.__reactInternalMemoizedMergedChildContext;
            break a;
          }
      }
      b = b.return;
    } while (b !== null);
    throw Error(p2(171));
  }
  if (a.tag === 1) {
    var c = a.type;
    if (Zf(c))
      return bg(a, c, b);
  }
  return b;
}
function el(a, b, c, d, e, f, g, h, k) {
  a = bl(c, d, true, a, e, f, g, h, k);
  a.context = dl(null);
  c = a.current;
  d = R2();
  e = yi(c);
  f = mh(d, e);
  f.callback = b !== undefined && b !== null ? b : null;
  nh(c, f, e);
  a.current.lanes = e;
  Ac(a, e, d);
  Dk(a, d);
  return a;
}
function fl(a, b, c, d) {
  var e = b.current, f = R2(), g = yi(e);
  c = dl(c);
  b.context === null ? b.context = c : b.pendingContext = c;
  b = mh(f, g);
  b.payload = { element: a };
  d = d === undefined ? null : d;
  d !== null && (b.callback = d);
  a = nh(e, b, g);
  a !== null && (gi(a, e, g, f), oh(a, e, g));
  return g;
}
function gl(a) {
  a = a.current;
  if (!a.child)
    return null;
  switch (a.child.tag) {
    case 5:
      return a.child.stateNode;
    default:
      return a.child.stateNode;
  }
}
function hl(a, b) {
  a = a.memoizedState;
  if (a !== null && a.dehydrated !== null) {
    var c = a.retryLane;
    a.retryLane = c !== 0 && c < b ? c : b;
  }
}
function il(a, b) {
  hl(a, b);
  (a = a.alternate) && hl(a, b);
}
function jl() {
  return null;
}
function ll(a) {
  this._internalRoot = a;
}
function ml(a) {
  this._internalRoot = a;
}
function nl(a) {
  return !(!a || a.nodeType !== 1 && a.nodeType !== 9 && a.nodeType !== 11);
}
function ol(a) {
  return !(!a || a.nodeType !== 1 && a.nodeType !== 9 && a.nodeType !== 11 && (a.nodeType !== 8 || a.nodeValue !== " react-mount-point-unstable "));
}
function pl() {}
function ql(a, b, c, d, e) {
  if (e) {
    if (typeof d === "function") {
      var f = d;
      d = function() {
        var a2 = gl(g);
        f.call(a2);
      };
    }
    var g = el(b, d, a, 0, null, false, false, "", pl);
    a._reactRootContainer = g;
    a[uf] = g.current;
    sf(a.nodeType === 8 ? a.parentNode : a);
    Rk();
    return g;
  }
  for (;e = a.lastChild; )
    a.removeChild(e);
  if (typeof d === "function") {
    var h = d;
    d = function() {
      var a2 = gl(k);
      h.call(a2);
    };
  }
  var k = bl(a, 0, false, null, null, false, false, "", pl);
  a._reactRootContainer = k;
  a[uf] = k.current;
  sf(a.nodeType === 8 ? a.parentNode : a);
  Rk(function() {
    fl(b, k, c, d);
  });
  return k;
}
function rl(a, b, c, d, e) {
  var f = c._reactRootContainer;
  if (f) {
    var g = f;
    if (typeof e === "function") {
      var h = e;
      e = function() {
        var a2 = gl(g);
        h.call(a2);
      };
    }
    fl(b, g, a, e);
  } else
    g = ql(c, b, a, e, d);
  return gl(g);
}
var aa, ca, da, ea, ia, ja, ka, la, ma, z2, ra, ua, va, wa, ya, za, Aa, Ba, Ca, Da, Ea, Fa, Ga, Ha, Ia, Ja, A2, La, Na = false, eb, mb, nb, pb, qb, tb, wb = null, yb = null, zb = null, Ab = null, Ib = false, Lb = false, Mb, Ob = false, Pb = null, Qb = false, Rb = null, Sb, ac, bc, cc, dc, B2, ec, fc, gc, hc, ic, jc, kc = null, lc = null, oc, pc, qc, rc = 64, sc = 4194304, C2 = 0, Ec, Fc, Gc, Hc, Ic, Jc = false, Kc, Lc = null, Mc = null, Nc = null, Oc, Pc, Qc, Rc, cd, dd = true, id = null, kd = null, ld = null, md = null, sd, td, ud, vd, wd, xd, yd, Ad, Bd, Cd, Dd, Ed, Fd, Gd, Hd, Id, Jd, Kd, Ld, Md, Nd, Od, Qd, Rd, Sd, Td, Ud, Vd, Wd, Xd, Yd, Zd, $d, ae, be = null, ce, de, ee, fe = false, ie = false, le, pe = null, qe = null, we = false, xe, ye, ze, He, Pe, Qe = null, Re = null, Se = null, Te = false, We, Xe, Ye, $e, af, bf, cf, df, ef, hf, jf, kf, gf, lf, mf, rf, xf, yf, Cf = null, Df = null, Ff, Gf, Hf, Jf, Nf, Of, Pf, uf, of, Qf, Rf, Sf, Tf = -1, Vf, H2, Wf, Xf, eg = null, fg = false, gg = false, kg, lg = 0, mg = null, ng = 0, og, pg = 0, qg = null, rg = 1, sg = "", xg = null, yg = null, I2 = false, zg = null, Kg, Ug, Vg, Wg, Xg = null, Yg = null, Zg = null, fh = null, jh = false, th, uh, vh, wh, L2, Dh, Fh, Gh, Hh = 0, M2 = null, N2 = null, O2 = null, Ih = false, Jh = false, Kh = 0, Lh = 0, Rh, Oh, Ph, Qh, Ei, Mi, Wi, dh = false, mj, zj, Aj, Bj, Cj, Jj = false, U2 = false, Kj, V2 = null, Nj = false, X2 = null, Xj = false, lk, mk, nk, ok, K2 = 0, Q2 = null, Y = null, Z = 0, fj = 0, ej, T2 = 0, pk = null, rh = 0, qk = 0, rk = 0, sk = null, tk = null, fk = 0, Gj = Infinity, uk = null, Oi = false, Pi = null, Ri = null, vk = false, wk = null, xk = 0, yk = 0, zk = null, Ak = -1, Bk = 0, Vk, kl, sl, tl, ul, vl, $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED2, $createPortal = function(a, b) {
  var c = 2 < arguments.length && arguments[2] !== undefined ? arguments[2] : null;
  if (!nl(b))
    throw Error(p2(200));
  return cl(a, b, null, c);
}, $createRoot = function(a, b) {
  if (!nl(a))
    throw Error(p2(299));
  var c = false, d = "", e = kl;
  b !== null && b !== undefined && (b.unstable_strictMode === true && (c = true), b.identifierPrefix !== undefined && (d = b.identifierPrefix), b.onRecoverableError !== undefined && (e = b.onRecoverableError));
  b = bl(a, 1, false, null, null, c, false, d, e);
  a[uf] = b.current;
  sf(a.nodeType === 8 ? a.parentNode : a);
  return new ll(b);
}, $findDOMNode = function(a) {
  if (a == null)
    return null;
  if (a.nodeType === 1)
    return a;
  var b = a._reactInternals;
  if (b === undefined) {
    if (typeof a.render === "function")
      throw Error(p2(188));
    a = Object.keys(a).join(",");
    throw Error(p2(268, a));
  }
  a = Zb(b);
  a = a === null ? null : a.stateNode;
  return a;
}, $flushSync = function(a) {
  return Rk(a);
}, $hydrate = function(a, b, c) {
  if (!ol(b))
    throw Error(p2(200));
  return rl(null, a, b, true, c);
}, $hydrateRoot = function(a, b, c) {
  if (!nl(a))
    throw Error(p2(405));
  var d = c != null && c.hydratedSources || null, e = false, f = "", g = kl;
  c !== null && c !== undefined && (c.unstable_strictMode === true && (e = true), c.identifierPrefix !== undefined && (f = c.identifierPrefix), c.onRecoverableError !== undefined && (g = c.onRecoverableError));
  b = el(b, null, a, 1, c != null ? c : null, e, false, f, g);
  a[uf] = b.current;
  sf(a);
  if (d)
    for (a = 0;a < d.length; a++)
      c = d[a], e = c._getVersion, e = e(c._source), b.mutableSourceEagerHydrationData == null ? b.mutableSourceEagerHydrationData = [c, e] : b.mutableSourceEagerHydrationData.push(c, e);
  return new ml(b);
}, $render = function(a, b, c) {
  if (!ol(b))
    throw Error(p2(200));
  return rl(null, a, b, false, c);
}, $unmountComponentAtNode = function(a) {
  if (!ol(a))
    throw Error(p2(40));
  return a._reactRootContainer ? (Rk(function() {
    rl(null, null, a, false, function() {
      a._reactRootContainer = null;
      a[uf] = null;
    });
  }), true) : false;
}, $unstable_batchedUpdates, $unstable_renderSubtreeIntoContainer = function(a, b, c, d) {
  if (!ol(c))
    throw Error(p2(200));
  if (a == null || a._reactInternals === undefined)
    throw Error(p2(38));
  return rl(a, b, c, false, d);
}, $version2 = "18.3.1-next-f1338f8080-20240426";
var init_react_dom_production_min = __esm(() => {
  aa = __toESM(require_react(), 1);
  ca = __toESM(require_scheduler(), 1);
  da = new Set;
  ea = {};
  ia = !(typeof window === "undefined" || typeof window.document === "undefined" || typeof window.document.createElement === "undefined");
  ja = Object.prototype.hasOwnProperty;
  ka = /^[:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD][:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\-.0-9\u00B7\u0300-\u036F\u203F-\u2040]*$/;
  la = {};
  ma = {};
  z2 = {};
  "children dangerouslySetInnerHTML defaultValue defaultChecked innerHTML suppressContentEditableWarning suppressHydrationWarning style".split(" ").forEach(function(a) {
    z2[a] = new v2(a, 0, false, a, null, false, false);
  });
  [["acceptCharset", "accept-charset"], ["className", "class"], ["htmlFor", "for"], ["httpEquiv", "http-equiv"]].forEach(function(a) {
    var b = a[0];
    z2[b] = new v2(b, 1, false, a[1], null, false, false);
  });
  ["contentEditable", "draggable", "spellCheck", "value"].forEach(function(a) {
    z2[a] = new v2(a, 2, false, a.toLowerCase(), null, false, false);
  });
  ["autoReverse", "externalResourcesRequired", "focusable", "preserveAlpha"].forEach(function(a) {
    z2[a] = new v2(a, 2, false, a, null, false, false);
  });
  "allowFullScreen async autoFocus autoPlay controls default defer disabled disablePictureInPicture disableRemotePlayback formNoValidate hidden loop noModule noValidate open playsInline readOnly required reversed scoped seamless itemScope".split(" ").forEach(function(a) {
    z2[a] = new v2(a, 3, false, a.toLowerCase(), null, false, false);
  });
  ["checked", "multiple", "muted", "selected"].forEach(function(a) {
    z2[a] = new v2(a, 3, true, a, null, false, false);
  });
  ["capture", "download"].forEach(function(a) {
    z2[a] = new v2(a, 4, false, a, null, false, false);
  });
  ["cols", "rows", "size", "span"].forEach(function(a) {
    z2[a] = new v2(a, 6, false, a, null, false, false);
  });
  ["rowSpan", "start"].forEach(function(a) {
    z2[a] = new v2(a, 5, false, a.toLowerCase(), null, false, false);
  });
  ra = /[\-:]([a-z])/g;
  "accent-height alignment-baseline arabic-form baseline-shift cap-height clip-path clip-rule color-interpolation color-interpolation-filters color-profile color-rendering dominant-baseline enable-background fill-opacity fill-rule flood-color flood-opacity font-family font-size font-size-adjust font-stretch font-style font-variant font-weight glyph-name glyph-orientation-horizontal glyph-orientation-vertical horiz-adv-x horiz-origin-x image-rendering letter-spacing lighting-color marker-end marker-mid marker-start overline-position overline-thickness paint-order panose-1 pointer-events rendering-intent shape-rendering stop-color stop-opacity strikethrough-position strikethrough-thickness stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width text-anchor text-decoration text-rendering underline-position underline-thickness unicode-bidi unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical vector-effect vert-adv-y vert-origin-x vert-origin-y word-spacing writing-mode xmlns:xlink x-height".split(" ").forEach(function(a) {
    var b = a.replace(ra, sa);
    z2[b] = new v2(b, 1, false, a, null, false, false);
  });
  "xlink:actuate xlink:arcrole xlink:role xlink:show xlink:title xlink:type".split(" ").forEach(function(a) {
    var b = a.replace(ra, sa);
    z2[b] = new v2(b, 1, false, a, "http://www.w3.org/1999/xlink", false, false);
  });
  ["xml:base", "xml:lang", "xml:space"].forEach(function(a) {
    var b = a.replace(ra, sa);
    z2[b] = new v2(b, 1, false, a, "http://www.w3.org/XML/1998/namespace", false, false);
  });
  ["tabIndex", "crossOrigin"].forEach(function(a) {
    z2[a] = new v2(a, 1, false, a.toLowerCase(), null, false, false);
  });
  z2.xlinkHref = new v2("xlinkHref", 1, false, "xlink:href", "http://www.w3.org/1999/xlink", true, false);
  ["src", "href", "action", "formAction"].forEach(function(a) {
    z2[a] = new v2(a, 1, false, a.toLowerCase(), null, true, true);
  });
  ua = aa.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
  va = Symbol.for("react.element");
  wa = Symbol.for("react.portal");
  ya = Symbol.for("react.fragment");
  za = Symbol.for("react.strict_mode");
  Aa = Symbol.for("react.profiler");
  Ba = Symbol.for("react.provider");
  Ca = Symbol.for("react.context");
  Da = Symbol.for("react.forward_ref");
  Ea = Symbol.for("react.suspense");
  Fa = Symbol.for("react.suspense_list");
  Ga = Symbol.for("react.memo");
  Ha = Symbol.for("react.lazy");
  Symbol.for("react.scope");
  Symbol.for("react.debug_trace_mode");
  Ia = Symbol.for("react.offscreen");
  Symbol.for("react.legacy_hidden");
  Symbol.for("react.cache");
  Symbol.for("react.tracing_marker");
  Ja = Symbol.iterator;
  A2 = Object.assign;
  eb = Array.isArray;
  nb = function(a) {
    return typeof MSApp !== "undefined" && MSApp.execUnsafeLocalFunction ? function(b, c, d, e) {
      MSApp.execUnsafeLocalFunction(function() {
        return a(b, c, d, e);
      });
    } : a;
  }(function(a, b) {
    if (a.namespaceURI !== "http://www.w3.org/2000/svg" || "innerHTML" in a)
      a.innerHTML = b;
    else {
      mb = mb || document.createElement("div");
      mb.innerHTML = "<svg>" + b.valueOf().toString() + "</svg>";
      for (b = mb.firstChild;a.firstChild; )
        a.removeChild(a.firstChild);
      for (;b.firstChild; )
        a.appendChild(b.firstChild);
    }
  });
  pb = {
    animationIterationCount: true,
    aspectRatio: true,
    borderImageOutset: true,
    borderImageSlice: true,
    borderImageWidth: true,
    boxFlex: true,
    boxFlexGroup: true,
    boxOrdinalGroup: true,
    columnCount: true,
    columns: true,
    flex: true,
    flexGrow: true,
    flexPositive: true,
    flexShrink: true,
    flexNegative: true,
    flexOrder: true,
    gridArea: true,
    gridRow: true,
    gridRowEnd: true,
    gridRowSpan: true,
    gridRowStart: true,
    gridColumn: true,
    gridColumnEnd: true,
    gridColumnSpan: true,
    gridColumnStart: true,
    fontWeight: true,
    lineClamp: true,
    lineHeight: true,
    opacity: true,
    order: true,
    orphans: true,
    tabSize: true,
    widows: true,
    zIndex: true,
    zoom: true,
    fillOpacity: true,
    floodOpacity: true,
    stopOpacity: true,
    strokeDasharray: true,
    strokeDashoffset: true,
    strokeMiterlimit: true,
    strokeOpacity: true,
    strokeWidth: true
  };
  qb = ["Webkit", "ms", "Moz", "O"];
  Object.keys(pb).forEach(function(a) {
    qb.forEach(function(b) {
      b = b + a.charAt(0).toUpperCase() + a.substring(1);
      pb[b] = pb[a];
    });
  });
  tb = A2({ menuitem: true }, { area: true, base: true, br: true, col: true, embed: true, hr: true, img: true, input: true, keygen: true, link: true, meta: true, param: true, source: true, track: true, wbr: true });
  if (ia)
    try {
      Mb = {};
      Object.defineProperty(Mb, "passive", { get: function() {
        Lb = true;
      } });
      window.addEventListener("test", Mb, Mb);
      window.removeEventListener("test", Mb, Mb);
    } catch (a) {
      Lb = false;
    }
  Sb = { onError: function(a) {
    Ob = true;
    Pb = a;
  } };
  ac = ca.unstable_scheduleCallback;
  bc = ca.unstable_cancelCallback;
  cc = ca.unstable_shouldYield;
  dc = ca.unstable_requestPaint;
  B2 = ca.unstable_now;
  ec = ca.unstable_getCurrentPriorityLevel;
  fc = ca.unstable_ImmediatePriority;
  gc = ca.unstable_UserBlockingPriority;
  hc = ca.unstable_NormalPriority;
  ic = ca.unstable_LowPriority;
  jc = ca.unstable_IdlePriority;
  oc = Math.clz32 ? Math.clz32 : nc;
  pc = Math.log;
  qc = Math.LN2;
  Kc = [];
  Oc = new Map;
  Pc = new Map;
  Qc = [];
  Rc = "mousedown mouseup touchcancel touchend touchstart auxclick dblclick pointercancel pointerdown pointerup dragend dragstart drop compositionend compositionstart keydown keypress keyup input textInput copy cut paste click change contextmenu reset submit".split(" ");
  cd = ua.ReactCurrentBatchConfig;
  sd = { eventPhase: 0, bubbles: 0, cancelable: 0, timeStamp: function(a) {
    return a.timeStamp || Date.now();
  }, defaultPrevented: 0, isTrusted: 0 };
  td = rd(sd);
  ud = A2({}, sd, { view: 0, detail: 0 });
  vd = rd(ud);
  Ad = A2({}, ud, { screenX: 0, screenY: 0, clientX: 0, clientY: 0, pageX: 0, pageY: 0, ctrlKey: 0, shiftKey: 0, altKey: 0, metaKey: 0, getModifierState: zd, button: 0, buttons: 0, relatedTarget: function(a) {
    return a.relatedTarget === undefined ? a.fromElement === a.srcElement ? a.toElement : a.fromElement : a.relatedTarget;
  }, movementX: function(a) {
    if ("movementX" in a)
      return a.movementX;
    a !== yd && (yd && a.type === "mousemove" ? (wd = a.screenX - yd.screenX, xd = a.screenY - yd.screenY) : xd = wd = 0, yd = a);
    return wd;
  }, movementY: function(a) {
    return "movementY" in a ? a.movementY : xd;
  } });
  Bd = rd(Ad);
  Cd = A2({}, Ad, { dataTransfer: 0 });
  Dd = rd(Cd);
  Ed = A2({}, ud, { relatedTarget: 0 });
  Fd = rd(Ed);
  Gd = A2({}, sd, { animationName: 0, elapsedTime: 0, pseudoElement: 0 });
  Hd = rd(Gd);
  Id = A2({}, sd, { clipboardData: function(a) {
    return "clipboardData" in a ? a.clipboardData : window.clipboardData;
  } });
  Jd = rd(Id);
  Kd = A2({}, sd, { data: 0 });
  Ld = rd(Kd);
  Md = {
    Esc: "Escape",
    Spacebar: " ",
    Left: "ArrowLeft",
    Up: "ArrowUp",
    Right: "ArrowRight",
    Down: "ArrowDown",
    Del: "Delete",
    Win: "OS",
    Menu: "ContextMenu",
    Apps: "ContextMenu",
    Scroll: "ScrollLock",
    MozPrintableKey: "Unidentified"
  };
  Nd = {
    8: "Backspace",
    9: "Tab",
    12: "Clear",
    13: "Enter",
    16: "Shift",
    17: "Control",
    18: "Alt",
    19: "Pause",
    20: "CapsLock",
    27: "Escape",
    32: " ",
    33: "PageUp",
    34: "PageDown",
    35: "End",
    36: "Home",
    37: "ArrowLeft",
    38: "ArrowUp",
    39: "ArrowRight",
    40: "ArrowDown",
    45: "Insert",
    46: "Delete",
    112: "F1",
    113: "F2",
    114: "F3",
    115: "F4",
    116: "F5",
    117: "F6",
    118: "F7",
    119: "F8",
    120: "F9",
    121: "F10",
    122: "F11",
    123: "F12",
    144: "NumLock",
    145: "ScrollLock",
    224: "Meta"
  };
  Od = { Alt: "altKey", Control: "ctrlKey", Meta: "metaKey", Shift: "shiftKey" };
  Qd = A2({}, ud, { key: function(a) {
    if (a.key) {
      var b = Md[a.key] || a.key;
      if (b !== "Unidentified")
        return b;
    }
    return a.type === "keypress" ? (a = od(a), a === 13 ? "Enter" : String.fromCharCode(a)) : a.type === "keydown" || a.type === "keyup" ? Nd[a.keyCode] || "Unidentified" : "";
  }, code: 0, location: 0, ctrlKey: 0, shiftKey: 0, altKey: 0, metaKey: 0, repeat: 0, locale: 0, getModifierState: zd, charCode: function(a) {
    return a.type === "keypress" ? od(a) : 0;
  }, keyCode: function(a) {
    return a.type === "keydown" || a.type === "keyup" ? a.keyCode : 0;
  }, which: function(a) {
    return a.type === "keypress" ? od(a) : a.type === "keydown" || a.type === "keyup" ? a.keyCode : 0;
  } });
  Rd = rd(Qd);
  Sd = A2({}, Ad, { pointerId: 0, width: 0, height: 0, pressure: 0, tangentialPressure: 0, tiltX: 0, tiltY: 0, twist: 0, pointerType: 0, isPrimary: 0 });
  Td = rd(Sd);
  Ud = A2({}, ud, { touches: 0, targetTouches: 0, changedTouches: 0, altKey: 0, metaKey: 0, ctrlKey: 0, shiftKey: 0, getModifierState: zd });
  Vd = rd(Ud);
  Wd = A2({}, sd, { propertyName: 0, elapsedTime: 0, pseudoElement: 0 });
  Xd = rd(Wd);
  Yd = A2({}, Ad, {
    deltaX: function(a) {
      return "deltaX" in a ? a.deltaX : ("wheelDeltaX" in a) ? -a.wheelDeltaX : 0;
    },
    deltaY: function(a) {
      return "deltaY" in a ? a.deltaY : ("wheelDeltaY" in a) ? -a.wheelDeltaY : ("wheelDelta" in a) ? -a.wheelDelta : 0;
    },
    deltaZ: 0,
    deltaMode: 0
  });
  Zd = rd(Yd);
  $d = [9, 13, 27, 32];
  ae = ia && "CompositionEvent" in window;
  ia && "documentMode" in document && (be = document.documentMode);
  ce = ia && "TextEvent" in window && !be;
  de = ia && (!ae || be && 8 < be && 11 >= be);
  ee = String.fromCharCode(32);
  le = { color: true, date: true, datetime: true, "datetime-local": true, email: true, month: true, number: true, password: true, range: true, search: true, tel: true, text: true, time: true, url: true, week: true };
  if (ia) {
    if (ia) {
      ye = "oninput" in document;
      if (!ye) {
        ze = document.createElement("div");
        ze.setAttribute("oninput", "return;");
        ye = typeof ze.oninput === "function";
      }
      xe = ye;
    } else
      xe = false;
    we = xe && (!document.documentMode || 9 < document.documentMode);
  }
  He = typeof Object.is === "function" ? Object.is : Ge;
  Pe = ia && "documentMode" in document && 11 >= document.documentMode;
  We = { animationend: Ve("Animation", "AnimationEnd"), animationiteration: Ve("Animation", "AnimationIteration"), animationstart: Ve("Animation", "AnimationStart"), transitionend: Ve("Transition", "TransitionEnd") };
  Xe = {};
  Ye = {};
  ia && (Ye = document.createElement("div").style, ("AnimationEvent" in window) || (delete We.animationend.animation, delete We.animationiteration.animation, delete We.animationstart.animation), ("TransitionEvent" in window) || delete We.transitionend.transition);
  $e = Ze("animationend");
  af = Ze("animationiteration");
  bf = Ze("animationstart");
  cf = Ze("transitionend");
  df = new Map;
  ef = "abort auxClick cancel canPlay canPlayThrough click close contextMenu copy cut drag dragEnd dragEnter dragExit dragLeave dragOver dragStart drop durationChange emptied encrypted ended error gotPointerCapture input invalid keyDown keyPress keyUp load loadedData loadedMetadata loadStart lostPointerCapture mouseDown mouseMove mouseOut mouseOver mouseUp paste pause play playing pointerCancel pointerDown pointerMove pointerOut pointerOver pointerUp progress rateChange reset resize seeked seeking stalled submit suspend timeUpdate touchCancel touchEnd touchStart volumeChange scroll toggle touchMove waiting wheel".split(" ");
  for (gf = 0;gf < ef.length; gf++) {
    hf = ef[gf], jf = hf.toLowerCase(), kf = hf[0].toUpperCase() + hf.slice(1);
    ff(jf, "on" + kf);
  }
  ff($e, "onAnimationEnd");
  ff(af, "onAnimationIteration");
  ff(bf, "onAnimationStart");
  ff("dblclick", "onDoubleClick");
  ff("focusin", "onFocus");
  ff("focusout", "onBlur");
  ff(cf, "onTransitionEnd");
  ha("onMouseEnter", ["mouseout", "mouseover"]);
  ha("onMouseLeave", ["mouseout", "mouseover"]);
  ha("onPointerEnter", ["pointerout", "pointerover"]);
  ha("onPointerLeave", ["pointerout", "pointerover"]);
  fa("onChange", "change click focusin focusout input keydown keyup selectionchange".split(" "));
  fa("onSelect", "focusout contextmenu dragend focusin keydown keyup mousedown mouseup selectionchange".split(" "));
  fa("onBeforeInput", ["compositionend", "keypress", "textInput", "paste"]);
  fa("onCompositionEnd", "compositionend focusout keydown keypress keyup mousedown".split(" "));
  fa("onCompositionStart", "compositionstart focusout keydown keypress keyup mousedown".split(" "));
  fa("onCompositionUpdate", "compositionupdate focusout keydown keypress keyup mousedown".split(" "));
  lf = "abort canplay canplaythrough durationchange emptied encrypted ended error loadeddata loadedmetadata loadstart pause play playing progress ratechange resize seeked seeking stalled suspend timeupdate volumechange waiting".split(" ");
  mf = new Set("cancel close invalid load scroll toggle".split(" ").concat(lf));
  rf = "_reactListening" + Math.random().toString(36).slice(2);
  xf = /\r\n?/g;
  yf = /\u0000|\uFFFD/g;
  Ff = typeof setTimeout === "function" ? setTimeout : undefined;
  Gf = typeof clearTimeout === "function" ? clearTimeout : undefined;
  Hf = typeof Promise === "function" ? Promise : undefined;
  Jf = typeof queueMicrotask === "function" ? queueMicrotask : typeof Hf !== "undefined" ? function(a) {
    return Hf.resolve(null).then(a).catch(If);
  } : Ff;
  Nf = Math.random().toString(36).slice(2);
  Of = "__reactFiber$" + Nf;
  Pf = "__reactProps$" + Nf;
  uf = "__reactContainer$" + Nf;
  of = "__reactEvents$" + Nf;
  Qf = "__reactListeners$" + Nf;
  Rf = "__reactHandles$" + Nf;
  Sf = [];
  Vf = {};
  H2 = Uf(Vf);
  Wf = Uf(false);
  Xf = Vf;
  kg = [];
  og = [];
  Kg = ua.ReactCurrentBatchConfig;
  Ug = Og(true);
  Vg = Og(false);
  Wg = Uf(null);
  th = {};
  uh = Uf(th);
  vh = Uf(th);
  wh = Uf(th);
  L2 = Uf(0);
  Dh = [];
  Fh = ua.ReactCurrentDispatcher;
  Gh = ua.ReactCurrentBatchConfig;
  Rh = { readContext: eh, useCallback: P2, useContext: P2, useEffect: P2, useImperativeHandle: P2, useInsertionEffect: P2, useLayoutEffect: P2, useMemo: P2, useReducer: P2, useRef: P2, useState: P2, useDebugValue: P2, useDeferredValue: P2, useTransition: P2, useMutableSource: P2, useSyncExternalStore: P2, useId: P2, unstable_isNewReconciler: false };
  Oh = { readContext: eh, useCallback: function(a, b) {
    Th().memoizedState = [a, b === undefined ? null : b];
    return a;
  }, useContext: eh, useEffect: mi, useImperativeHandle: function(a, b, c) {
    c = c !== null && c !== undefined ? c.concat([a]) : null;
    return ki(4194308, 4, pi.bind(null, b, a), c);
  }, useLayoutEffect: function(a, b) {
    return ki(4194308, 4, a, b);
  }, useInsertionEffect: function(a, b) {
    return ki(4, 2, a, b);
  }, useMemo: function(a, b) {
    var c = Th();
    b = b === undefined ? null : b;
    a = a();
    c.memoizedState = [a, b];
    return a;
  }, useReducer: function(a, b, c) {
    var d = Th();
    b = c !== undefined ? c(b) : b;
    d.memoizedState = d.baseState = b;
    a = { pending: null, interleaved: null, lanes: 0, dispatch: null, lastRenderedReducer: a, lastRenderedState: b };
    d.queue = a;
    a = a.dispatch = xi.bind(null, M2, a);
    return [d.memoizedState, a];
  }, useRef: function(a) {
    var b = Th();
    a = { current: a };
    return b.memoizedState = a;
  }, useState: hi, useDebugValue: ri, useDeferredValue: function(a) {
    return Th().memoizedState = a;
  }, useTransition: function() {
    var a = hi(false), b = a[0];
    a = vi.bind(null, a[1]);
    Th().memoizedState = a;
    return [b, a];
  }, useMutableSource: function() {}, useSyncExternalStore: function(a, b, c) {
    var d = M2, e = Th();
    if (I2) {
      if (c === undefined)
        throw Error(p2(407));
      c = c();
    } else {
      c = b();
      if (Q2 === null)
        throw Error(p2(349));
      (Hh & 30) !== 0 || di(d, b, c);
    }
    e.memoizedState = c;
    var f = { value: c, getSnapshot: b };
    e.queue = f;
    mi(ai.bind(null, d, f, a), [a]);
    d.flags |= 2048;
    bi(9, ci.bind(null, d, f, c, b), undefined, null);
    return c;
  }, useId: function() {
    var a = Th(), b = Q2.identifierPrefix;
    if (I2) {
      var c = sg;
      var d = rg;
      c = (d & ~(1 << 32 - oc(d) - 1)).toString(32) + c;
      b = ":" + b + "R" + c;
      c = Kh++;
      0 < c && (b += "H" + c.toString(32));
      b += ":";
    } else
      c = Lh++, b = ":" + b + "r" + c.toString(32) + ":";
    return a.memoizedState = b;
  }, unstable_isNewReconciler: false };
  Ph = {
    readContext: eh,
    useCallback: si,
    useContext: eh,
    useEffect: $h,
    useImperativeHandle: qi,
    useInsertionEffect: ni,
    useLayoutEffect: oi,
    useMemo: ti,
    useReducer: Wh,
    useRef: ji,
    useState: function() {
      return Wh(Vh);
    },
    useDebugValue: ri,
    useDeferredValue: function(a) {
      var b = Uh();
      return ui(b, N2.memoizedState, a);
    },
    useTransition: function() {
      var a = Wh(Vh)[0], b = Uh().memoizedState;
      return [a, b];
    },
    useMutableSource: Yh,
    useSyncExternalStore: Zh,
    useId: wi,
    unstable_isNewReconciler: false
  };
  Qh = { readContext: eh, useCallback: si, useContext: eh, useEffect: $h, useImperativeHandle: qi, useInsertionEffect: ni, useLayoutEffect: oi, useMemo: ti, useReducer: Xh, useRef: ji, useState: function() {
    return Xh(Vh);
  }, useDebugValue: ri, useDeferredValue: function(a) {
    var b = Uh();
    return N2 === null ? b.memoizedState = a : ui(b, N2.memoizedState, a);
  }, useTransition: function() {
    var a = Xh(Vh)[0], b = Uh().memoizedState;
    return [a, b];
  }, useMutableSource: Yh, useSyncExternalStore: Zh, useId: wi, unstable_isNewReconciler: false };
  Ei = { isMounted: function(a) {
    return (a = a._reactInternals) ? Vb(a) === a : false;
  }, enqueueSetState: function(a, b, c) {
    a = a._reactInternals;
    var d = R2(), e = yi(a), f = mh(d, e);
    f.payload = b;
    c !== undefined && c !== null && (f.callback = c);
    b = nh(a, f, e);
    b !== null && (gi(b, a, e, d), oh(b, a, e));
  }, enqueueReplaceState: function(a, b, c) {
    a = a._reactInternals;
    var d = R2(), e = yi(a), f = mh(d, e);
    f.tag = 1;
    f.payload = b;
    c !== undefined && c !== null && (f.callback = c);
    b = nh(a, f, e);
    b !== null && (gi(b, a, e, d), oh(b, a, e));
  }, enqueueForceUpdate: function(a, b) {
    a = a._reactInternals;
    var c = R2(), d = yi(a), e = mh(c, d);
    e.tag = 2;
    b !== undefined && b !== null && (e.callback = b);
    b = nh(a, e, d);
    b !== null && (gi(b, a, d, c), oh(b, a, d));
  } };
  Mi = typeof WeakMap === "function" ? WeakMap : Map;
  Wi = ua.ReactCurrentOwner;
  mj = { dehydrated: null, treeContext: null, retryLane: 0 };
  zj = function(a, b) {
    for (var c = b.child;c !== null; ) {
      if (c.tag === 5 || c.tag === 6)
        a.appendChild(c.stateNode);
      else if (c.tag !== 4 && c.child !== null) {
        c.child.return = c;
        c = c.child;
        continue;
      }
      if (c === b)
        break;
      for (;c.sibling === null; ) {
        if (c.return === null || c.return === b)
          return;
        c = c.return;
      }
      c.sibling.return = c.return;
      c = c.sibling;
    }
  };
  Aj = function() {};
  Bj = function(a, b, c, d) {
    var e = a.memoizedProps;
    if (e !== d) {
      a = b.stateNode;
      xh(uh.current);
      var f = null;
      switch (c) {
        case "input":
          e = Ya(a, e);
          d = Ya(a, d);
          f = [];
          break;
        case "select":
          e = A2({}, e, { value: undefined });
          d = A2({}, d, { value: undefined });
          f = [];
          break;
        case "textarea":
          e = gb(a, e);
          d = gb(a, d);
          f = [];
          break;
        default:
          typeof e.onClick !== "function" && typeof d.onClick === "function" && (a.onclick = Bf);
      }
      ub(c, d);
      var g;
      c = null;
      for (l2 in e)
        if (!d.hasOwnProperty(l2) && e.hasOwnProperty(l2) && e[l2] != null)
          if (l2 === "style") {
            var h = e[l2];
            for (g in h)
              h.hasOwnProperty(g) && (c || (c = {}), c[g] = "");
          } else
            l2 !== "dangerouslySetInnerHTML" && l2 !== "children" && l2 !== "suppressContentEditableWarning" && l2 !== "suppressHydrationWarning" && l2 !== "autoFocus" && (ea.hasOwnProperty(l2) ? f || (f = []) : (f = f || []).push(l2, null));
      for (l2 in d) {
        var k = d[l2];
        h = e != null ? e[l2] : undefined;
        if (d.hasOwnProperty(l2) && k !== h && (k != null || h != null))
          if (l2 === "style")
            if (h) {
              for (g in h)
                !h.hasOwnProperty(g) || k && k.hasOwnProperty(g) || (c || (c = {}), c[g] = "");
              for (g in k)
                k.hasOwnProperty(g) && h[g] !== k[g] && (c || (c = {}), c[g] = k[g]);
            } else
              c || (f || (f = []), f.push(l2, c)), c = k;
          else
            l2 === "dangerouslySetInnerHTML" ? (k = k ? k.__html : undefined, h = h ? h.__html : undefined, k != null && h !== k && (f = f || []).push(l2, k)) : l2 === "children" ? typeof k !== "string" && typeof k !== "number" || (f = f || []).push(l2, "" + k) : l2 !== "suppressContentEditableWarning" && l2 !== "suppressHydrationWarning" && (ea.hasOwnProperty(l2) ? (k != null && l2 === "onScroll" && D2("scroll", a), f || h === k || (f = [])) : (f = f || []).push(l2, k));
      }
      c && (f = f || []).push("style", c);
      var l2 = f;
      if (b.updateQueue = l2)
        b.flags |= 4;
    }
  };
  Cj = function(a, b, c, d) {
    c !== d && (b.flags |= 4);
  };
  Kj = typeof WeakSet === "function" ? WeakSet : Set;
  lk = Math.ceil;
  mk = ua.ReactCurrentDispatcher;
  nk = ua.ReactCurrentOwner;
  ok = ua.ReactCurrentBatchConfig;
  ej = Uf(0);
  Vk = function(a, b, c) {
    if (a !== null)
      if (a.memoizedProps !== b.pendingProps || Wf.current)
        dh = true;
      else {
        if ((a.lanes & c) === 0 && (b.flags & 128) === 0)
          return dh = false, yj(a, b, c);
        dh = (a.flags & 131072) !== 0 ? true : false;
      }
    else
      dh = false, I2 && (b.flags & 1048576) !== 0 && ug(b, ng, b.index);
    b.lanes = 0;
    switch (b.tag) {
      case 2:
        var d = b.type;
        ij(a, b);
        a = b.pendingProps;
        var e = Yf(b, H2.current);
        ch(b, c);
        e = Nh(null, b, d, a, e, c);
        var f = Sh();
        b.flags |= 1;
        typeof e === "object" && e !== null && typeof e.render === "function" && e.$$typeof === undefined ? (b.tag = 1, b.memoizedState = null, b.updateQueue = null, Zf(d) ? (f = true, cg(b)) : f = false, b.memoizedState = e.state !== null && e.state !== undefined ? e.state : null, kh(b), e.updater = Ei, b.stateNode = e, e._reactInternals = b, Ii(b, d, a, c), b = jj(null, b, d, true, f, c)) : (b.tag = 0, I2 && f && vg(b), Xi(null, b, e, c), b = b.child);
        return b;
      case 16:
        d = b.elementType;
        a: {
          ij(a, b);
          a = b.pendingProps;
          e = d._init;
          d = e(d._payload);
          b.type = d;
          e = b.tag = Zk(d);
          a = Ci(d, a);
          switch (e) {
            case 0:
              b = cj(null, b, d, a, c);
              break a;
            case 1:
              b = hj(null, b, d, a, c);
              break a;
            case 11:
              b = Yi(null, b, d, a, c);
              break a;
            case 14:
              b = $i(null, b, d, Ci(d.type, a), c);
              break a;
          }
          throw Error(p2(306, d, ""));
        }
        return b;
      case 0:
        return d = b.type, e = b.pendingProps, e = b.elementType === d ? e : Ci(d, e), cj(a, b, d, e, c);
      case 1:
        return d = b.type, e = b.pendingProps, e = b.elementType === d ? e : Ci(d, e), hj(a, b, d, e, c);
      case 3:
        a: {
          kj(b);
          if (a === null)
            throw Error(p2(387));
          d = b.pendingProps;
          f = b.memoizedState;
          e = f.element;
          lh(a, b);
          qh(b, d, null, c);
          var g = b.memoizedState;
          d = g.element;
          if (f.isDehydrated)
            if (f = { element: d, isDehydrated: false, cache: g.cache, pendingSuspenseBoundaries: g.pendingSuspenseBoundaries, transitions: g.transitions }, b.updateQueue.baseState = f, b.memoizedState = f, b.flags & 256) {
              e = Ji(Error(p2(423)), b);
              b = lj(a, b, d, c, e);
              break a;
            } else if (d !== e) {
              e = Ji(Error(p2(424)), b);
              b = lj(a, b, d, c, e);
              break a;
            } else
              for (yg = Lf(b.stateNode.containerInfo.firstChild), xg = b, I2 = true, zg = null, c = Vg(b, null, d, c), b.child = c;c; )
                c.flags = c.flags & -3 | 4096, c = c.sibling;
          else {
            Ig();
            if (d === e) {
              b = Zi(a, b, c);
              break a;
            }
            Xi(a, b, d, c);
          }
          b = b.child;
        }
        return b;
      case 5:
        return Ah(b), a === null && Eg(b), d = b.type, e = b.pendingProps, f = a !== null ? a.memoizedProps : null, g = e.children, Ef(d, e) ? g = null : f !== null && Ef(d, f) && (b.flags |= 32), gj(a, b), Xi(a, b, g, c), b.child;
      case 6:
        return a === null && Eg(b), null;
      case 13:
        return oj(a, b, c);
      case 4:
        return yh(b, b.stateNode.containerInfo), d = b.pendingProps, a === null ? b.child = Ug(b, null, d, c) : Xi(a, b, d, c), b.child;
      case 11:
        return d = b.type, e = b.pendingProps, e = b.elementType === d ? e : Ci(d, e), Yi(a, b, d, e, c);
      case 7:
        return Xi(a, b, b.pendingProps, c), b.child;
      case 8:
        return Xi(a, b, b.pendingProps.children, c), b.child;
      case 12:
        return Xi(a, b, b.pendingProps.children, c), b.child;
      case 10:
        a: {
          d = b.type._context;
          e = b.pendingProps;
          f = b.memoizedProps;
          g = e.value;
          G2(Wg, d._currentValue);
          d._currentValue = g;
          if (f !== null)
            if (He(f.value, g)) {
              if (f.children === e.children && !Wf.current) {
                b = Zi(a, b, c);
                break a;
              }
            } else
              for (f = b.child, f !== null && (f.return = b);f !== null; ) {
                var h = f.dependencies;
                if (h !== null) {
                  g = f.child;
                  for (var k = h.firstContext;k !== null; ) {
                    if (k.context === d) {
                      if (f.tag === 1) {
                        k = mh(-1, c & -c);
                        k.tag = 2;
                        var l2 = f.updateQueue;
                        if (l2 !== null) {
                          l2 = l2.shared;
                          var m = l2.pending;
                          m === null ? k.next = k : (k.next = m.next, m.next = k);
                          l2.pending = k;
                        }
                      }
                      f.lanes |= c;
                      k = f.alternate;
                      k !== null && (k.lanes |= c);
                      bh(f.return, c, b);
                      h.lanes |= c;
                      break;
                    }
                    k = k.next;
                  }
                } else if (f.tag === 10)
                  g = f.type === b.type ? null : f.child;
                else if (f.tag === 18) {
                  g = f.return;
                  if (g === null)
                    throw Error(p2(341));
                  g.lanes |= c;
                  h = g.alternate;
                  h !== null && (h.lanes |= c);
                  bh(g, c, b);
                  g = f.sibling;
                } else
                  g = f.child;
                if (g !== null)
                  g.return = f;
                else
                  for (g = f;g !== null; ) {
                    if (g === b) {
                      g = null;
                      break;
                    }
                    f = g.sibling;
                    if (f !== null) {
                      f.return = g.return;
                      g = f;
                      break;
                    }
                    g = g.return;
                  }
                f = g;
              }
          Xi(a, b, e.children, c);
          b = b.child;
        }
        return b;
      case 9:
        return e = b.type, d = b.pendingProps.children, ch(b, c), e = eh(e), d = d(e), b.flags |= 1, Xi(a, b, d, c), b.child;
      case 14:
        return d = b.type, e = Ci(d, b.pendingProps), e = Ci(d.type, e), $i(a, b, d, e, c);
      case 15:
        return bj(a, b, b.type, b.pendingProps, c);
      case 17:
        return d = b.type, e = b.pendingProps, e = b.elementType === d ? e : Ci(d, e), ij(a, b), b.tag = 1, Zf(d) ? (a = true, cg(b)) : a = false, ch(b, c), Gi(b, d, e), Ii(b, d, e, c), jj(null, b, d, true, a, c);
      case 19:
        return xj(a, b, c);
      case 22:
        return dj(a, b, c);
    }
    throw Error(p2(156, b.tag));
  };
  kl = typeof reportError === "function" ? reportError : function(a) {
    console.error(a);
  };
  ml.prototype.render = ll.prototype.render = function(a) {
    var b = this._internalRoot;
    if (b === null)
      throw Error(p2(409));
    fl(a, b, null, null);
  };
  ml.prototype.unmount = ll.prototype.unmount = function() {
    var a = this._internalRoot;
    if (a !== null) {
      this._internalRoot = null;
      var b = a.containerInfo;
      Rk(function() {
        fl(null, a, null, null);
      });
      b[uf] = null;
    }
  };
  ml.prototype.unstable_scheduleHydration = function(a) {
    if (a) {
      var b = Hc();
      a = { blockedOn: null, target: a, priority: b };
      for (var c = 0;c < Qc.length && b !== 0 && b < Qc[c].priority; c++)
        ;
      Qc.splice(c, 0, a);
      c === 0 && Vc(a);
    }
  };
  Ec = function(a) {
    switch (a.tag) {
      case 3:
        var b = a.stateNode;
        if (b.current.memoizedState.isDehydrated) {
          var c = tc(b.pendingLanes);
          c !== 0 && (Cc(b, c | 1), Dk(b, B2()), (K2 & 6) === 0 && (Gj = B2() + 500, jg()));
        }
        break;
      case 13:
        Rk(function() {
          var b2 = ih(a, 1);
          if (b2 !== null) {
            var c2 = R2();
            gi(b2, a, 1, c2);
          }
        }), il(a, 1);
    }
  };
  Fc = function(a) {
    if (a.tag === 13) {
      var b = ih(a, 134217728);
      if (b !== null) {
        var c = R2();
        gi(b, a, 134217728, c);
      }
      il(a, 134217728);
    }
  };
  Gc = function(a) {
    if (a.tag === 13) {
      var b = yi(a), c = ih(a, b);
      if (c !== null) {
        var d = R2();
        gi(c, a, b, d);
      }
      il(a, b);
    }
  };
  Hc = function() {
    return C2;
  };
  Ic = function(a, b) {
    var c = C2;
    try {
      return C2 = a, b();
    } finally {
      C2 = c;
    }
  };
  yb = function(a, b, c) {
    switch (b) {
      case "input":
        bb(a, c);
        b = c.name;
        if (c.type === "radio" && b != null) {
          for (c = a;c.parentNode; )
            c = c.parentNode;
          c = c.querySelectorAll("input[name=" + JSON.stringify("" + b) + '][type="radio"]');
          for (b = 0;b < c.length; b++) {
            var d = c[b];
            if (d !== a && d.form === a.form) {
              var e = Db(d);
              if (!e)
                throw Error(p2(90));
              Wa(d);
              bb(d, e);
            }
          }
        }
        break;
      case "textarea":
        ib(a, c);
        break;
      case "select":
        b = c.value, b != null && fb(a, !!c.multiple, b, false);
    }
  };
  Gb = Qk;
  Hb = Rk;
  sl = { usingClientEntryPoint: false, Events: [Cb, ue, Db, Eb, Fb, Qk] };
  tl = { findFiberByHostInstance: Wc, bundleType: 0, version: "18.3.1", rendererPackageName: "react-dom" };
  ul = { bundleType: tl.bundleType, version: tl.version, rendererPackageName: tl.rendererPackageName, rendererConfig: tl.rendererConfig, overrideHookState: null, overrideHookStateDeletePath: null, overrideHookStateRenamePath: null, overrideProps: null, overridePropsDeletePath: null, overridePropsRenamePath: null, setErrorHandler: null, setSuspenseHandler: null, scheduleUpdate: null, currentDispatcherRef: ua.ReactCurrentDispatcher, findHostInstanceByFiber: function(a) {
    a = Zb(a);
    return a === null ? null : a.stateNode;
  }, findFiberByHostInstance: tl.findFiberByHostInstance || jl, findHostInstancesForRefresh: null, scheduleRefresh: null, scheduleRoot: null, setRefreshHandler: null, getCurrentFiber: null, reconcilerVersion: "18.3.1-next-f1338f8080-20240426" };
  if (typeof __REACT_DEVTOOLS_GLOBAL_HOOK__ !== "undefined") {
    vl = __REACT_DEVTOOLS_GLOBAL_HOOK__;
    if (!vl.isDisabled && vl.supportsFiber)
      try {
        kc = vl.inject(ul), lc = vl;
      } catch (a) {}
  }
  $__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED2 = sl;
  $unstable_batchedUpdates = Qk;
});

// webapp/node_modules/react-dom/index.js
var require_react_dom = __commonJS((exports, module) => {
  init_react_dom_production_min();
  function checkDCE() {
    if (typeof __REACT_DEVTOOLS_GLOBAL_HOOK__ === "undefined" || typeof __REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE !== "function") {
      return;
    }
    if (false) {}
    try {
      __REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE(checkDCE);
    } catch (err) {
      console.error(err);
    }
  }
  if (true) {
    checkDCE();
    module.exports = exports_react_dom_production_min;
  }
});

// webapp/node_modules/react-dom/client.js
var require_client = __commonJS((exports) => {
  var m = __toESM(require_react_dom());
  if (true) {
    exports.createRoot = m.createRoot;
    exports.hydrateRoot = m.hydrateRoot;
  }
  var i;
});

// webapp/node_modules/react/cjs/react-jsx-runtime.production.min.js
var exports_react_jsx_runtime_production_min = {};
__export(exports_react_jsx_runtime_production_min, {
  jsxs: () => $jsxs,
  jsx: () => $jsx,
  Fragment: () => $Fragment2
});
function q2(c, a, g) {
  var b, d = {}, e = null, h = null;
  g !== undefined && (e = "" + g);
  a.key !== undefined && (e = "" + a.key);
  a.ref !== undefined && (h = a.ref);
  for (b in a)
    m.call(a, b) && !p3.hasOwnProperty(b) && (d[b] = a[b]);
  if (c && c.defaultProps)
    for (b in a = c.defaultProps, a)
      d[b] === undefined && (d[b] = a[b]);
  return { $$typeof: k, type: c, key: e, ref: h, props: d, _owner: n2.current };
}
var f, k, l2, m, n2, p3, $Fragment2, $jsx, $jsxs;
var init_react_jsx_runtime_production_min = __esm(() => {
  f = __toESM(require_react(), 1);
  k = Symbol.for("react.element");
  l2 = Symbol.for("react.fragment");
  m = Object.prototype.hasOwnProperty;
  n2 = f.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED.ReactCurrentOwner;
  p3 = { key: true, ref: true, __self: true, __source: true };
  $Fragment2 = l2;
  $jsx = q2;
  $jsxs = q2;
});

// webapp/node_modules/react/jsx-runtime.js
var require_jsx_runtime = __commonJS((exports, module) => {
  init_react_jsx_runtime_production_min();
  if (true) {
    module.exports = exports_react_jsx_runtime_production_min;
  }
});

// webapp/src/static-api.tsx
var import_react10 = __toESM(require_react(), 1);
var import_client = __toESM(require_client(), 1);
var import_react_dom = __toESM(require_react_dom(), 1);

// webapp/src/components/DataTable.tsx
var import_react = __toESM(require_react(), 1);

// webapp/src/lib/uiCore.js
function labelize(value) {
  return String(value || "").replaceAll("_", " ").replaceAll(".", " ").replace(/\b\w/g, (char) => char.toUpperCase()).trim();
}
function aliasForSemanticRef(ref) {
  return String(ref || "").split(".").at(-1);
}
function formatUiValue(value, options = {}) {
  if (value === null || value === undefined || value === "")
    return "—";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric))
    return String(value);
  const format = String(options.format || "").toLowerCase();
  const percent = options.style === "percent" || format.includes("%") || format.includes("percent") || format.includes("pct") || options.type === "ratio";
  if (percent) {
    const scaled = options.style === "percent" ? numeric : Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return scaled.toLocaleString(undefined, {
      maximumFractionDigits: options.maximumFractionDigits ?? 1,
      style: options.style === "percent" ? "percent" : "decimal"
    }) + (options.style === "percent" ? "" : "%");
  }
  const currency = options.style === "currency" || options.currency || format.includes("$") || format.includes("usd") || format.includes("currency") || format.includes("dollar");
  return numeric.toLocaleString(undefined, {
    currency: currency ? options.currency || "USD" : undefined,
    maximumFractionDigits: options.maximumFractionDigits ?? 2,
    notation: options.compact ? "compact" : "standard",
    style: currency ? "currency" : "decimal"
  });
}
function formatUiCompact(value, options = {}) {
  return formatUiValue(value, { ...options, compact: true, maximumFractionDigits: options.maximumFractionDigits ?? 1 });
}
function normalizeFilterValue(value) {
  return String(value ?? "");
}
function toggleFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const selected = new Set((next[dimension] || []).map(normalizeFilterValue));
  if (selected.has(normalized))
    selected.delete(normalized);
  else
    selected.add(normalized);
  if (selected.size)
    next[dimension] = [...selected];
  else
    delete next[dimension];
  return next;
}
function removeFilterDimension(filters, dimension) {
  const next = { ...filters };
  delete next[dimension];
  return next;
}
function removeFilterValue(filters, dimension, value) {
  const next = { ...filters };
  const normalized = normalizeFilterValue(value);
  const values = (next[dimension] || []).map(normalizeFilterValue).filter((item) => item !== normalized);
  if (values.length)
    next[dimension] = values;
  else
    delete next[dimension];
  return next;
}
function paginateRows(rows, page, pageSize) {
  const paginate = pageSize > 0 && rows.length > pageSize;
  const pageCount = paginate ? Math.ceil(rows.length / pageSize) : 1;
  const safePage = Math.max(0, Math.min(page, pageCount - 1));
  const start = paginate ? safePage * pageSize : 0;
  return {
    paginate,
    pageCount,
    safePage,
    start,
    visibleRows: paginate ? rows.slice(start, start + pageSize) : rows
  };
}

// webapp/src/components/DataTable.tsx
var jsx_runtime = __toESM(require_jsx_runtime(), 1);
function DataTable({ columns, rows, loading, sortKey, sortDir, onSort, renderCell, pageSize = 50 }) {
  const [page, setPage] = import_react.useState(0);
  const { paginate, pageCount, safePage, start, visibleRows } = paginateRows(rows, page, pageSize);
  import_react.useEffect(() => {
    setPage(0);
  }, [rows, pageSize, sortKey, sortDir]);
  return /* @__PURE__ */ jsx_runtime.jsxs("div", {
    className: "overflow-hidden border border-line bg-surface",
    children: [
      /* @__PURE__ */ jsx_runtime.jsx("div", {
        className: "overflow-auto",
        children: /* @__PURE__ */ jsx_runtime.jsxs("table", {
          className: "w-max min-w-full border-collapse text-xs",
          "data-testid": "pivot-table",
          children: [
            /* @__PURE__ */ jsx_runtime.jsx("thead", {
              children: /* @__PURE__ */ jsx_runtime.jsx("tr", {
                className: "bg-surface-soft",
                children: columns.map((column) => {
                  const active = sortKey === column.key;
                  return /* @__PURE__ */ jsx_runtime.jsx("th", {
                    className: `max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 font-semibold text-faint ${column.numeric ? "min-w-32 text-right" : "min-w-40 text-left"}`,
                    children: column.sortable && onSort ? /* @__PURE__ */ jsx_runtime.jsxs("button", {
                      type: "button",
                      onClick: () => onSort(column.key),
                      "aria-label": `Sort by ${column.label}${active ? `, currently ${sortDir === "asc" ? "ascending" : "descending"}` : ""}`,
                      className: `inline-flex min-h-11 max-w-full items-center gap-1 whitespace-nowrap hover:text-ink ${active ? "text-ink" : ""}`,
                      children: [
                        /* @__PURE__ */ jsx_runtime.jsx("span", {
                          className: "truncate",
                          children: column.label
                        }),
                        /* @__PURE__ */ jsx_runtime.jsx("span", {
                          "aria-hidden": "true",
                          className: "text-[9px]",
                          children: active ? sortDir === "asc" ? "▲" : "▼" : "↕"
                        })
                      ]
                    }) : /* @__PURE__ */ jsx_runtime.jsx("span", {
                      className: "block truncate",
                      title: column.label,
                      children: column.label
                    })
                  }, column.key);
                })
              })
            }),
            /* @__PURE__ */ jsx_runtime.jsx("tbody", {
              children: loading && rows.length === 0 ? /* @__PURE__ */ jsx_runtime.jsx("tr", {
                children: /* @__PURE__ */ jsx_runtime.jsx("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: "Loading…"
                })
              }) : rows.length === 0 ? /* @__PURE__ */ jsx_runtime.jsx("tr", {
                children: /* @__PURE__ */ jsx_runtime.jsx("td", {
                  colSpan: columns.length,
                  className: "px-3 py-6 text-center text-faint",
                  children: "No rows"
                })
              }) : visibleRows.map((row, index) => /* @__PURE__ */ jsx_runtime.jsx("tr", {
                className: "hover:bg-surface-soft",
                children: columns.map((column) => {
                  const cellText = renderCell(column, row[column.key]);
                  return /* @__PURE__ */ jsx_runtime.jsx("td", {
                    className: `max-w-80 whitespace-nowrap border-b border-line px-3 py-1.5 text-muted ${column.numeric ? "min-w-32 text-right font-mono tnum text-ink" : "min-w-40"}`,
                    children: /* @__PURE__ */ jsx_runtime.jsx("span", {
                      className: "block max-w-80 truncate",
                      title: cellText,
                      children: cellText
                    })
                  }, column.key);
                })
              }, start + index))
            })
          ]
        })
      }),
      paginate ? /* @__PURE__ */ jsx_runtime.jsxs("div", {
        "data-testid": "pivot-table-pager",
        className: "flex min-h-11 items-center justify-between gap-3 border-t border-line px-3 text-2xs text-faint",
        children: [
          /* @__PURE__ */ jsx_runtime.jsxs("span", {
            className: "tnum",
            children: [
              start + 1,
              "–",
              Math.min(start + pageSize, rows.length),
              " of ",
              rows.length.toLocaleString(),
              loading ? " · Updating…" : ""
            ]
          }),
          /* @__PURE__ */ jsx_runtime.jsxs("div", {
            className: "flex gap-1",
            children: [
              /* @__PURE__ */ jsx_runtime.jsx("button", {
                type: "button",
                disabled: safePage === 0,
                onClick: () => setPage((value) => Math.max(0, value - 1)),
                className: "min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
                children: "Prev"
              }),
              /* @__PURE__ */ jsx_runtime.jsx("button", {
                type: "button",
                disabled: safePage >= pageCount - 1,
                onClick: () => setPage((value) => Math.min(pageCount - 1, value + 1)),
                className: "min-h-11 min-w-11 px-2 text-muted hover:text-ink disabled:cursor-not-allowed disabled:opacity-40",
                children: "Next"
              })
            ]
          })
        ]
      }) : null
    ]
  });
}

// webapp/src/components/FilterPill.tsx
var import_react6 = __toESM(require_react(), 1);

// webapp/src/data/types.ts
function aliasOf(ref) {
  const last = ref.split(".").at(-1);
  return last || ref;
}
var NULL_TOKEN = "\x00__null__";

// webapp/src/lib/format.ts
function displayDimValue(value) {
  return value === NULL_TOKEN || value === "" ? "—" : value;
}
function formatValue(value, hint = {}) {
  return formatUiValue(value, hint);
}
function sqlLiteral(value) {
  return `'${value.replaceAll("'", "''")}'`;
}
function filterSummary(filter) {
  if (filter.mode === "contains")
    return `contains ${sqlLiteral(filter.pattern ?? "")}`;
  const { values } = filter;
  const verb = filter.mode === "exclude" ? "is not" : "is";
  if (values.length === 0)
    return verb;
  if (values.length === 1)
    return `${verb} ${displayDimValue(values[0])}`;
  if (values.length <= 2)
    return `${verb} ${values.map(displayDimValue).join(", ")}`;
  return `${verb} ${values.length} values`;
}

// webapp/src/components/FilterEditor.tsx
var import_react5 = __toESM(require_react(), 1);

// webapp/src/lib/time.ts
var ALL_GRAINS = ["hour", "day", "week", "month", "quarter", "year"];
function isoDate(date) {
  return date.toISOString().slice(0, 10);
}
function dateOnly(value) {
  return value.slice(0, 10);
}
function parseISO(value) {
  return new Date(`${dateOnly(value)}T00:00:00Z`);
}
function addDays(value, days) {
  const date = parseISO(value);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}
function timeFilters(ref, range) {
  return [`${ref} >= cast('${range.from}' as date)`, `${ref} < cast('${addDays(range.to, 1)}' as date)`];
}

// webapp/src/lib/queries.ts
function isEmptyFilter(filter) {
  return filter.mode === "contains" ? !filter.pattern : filter.values.length === 0;
}
function filterLiteral(value, type) {
  if ((type === "numeric" || type === "number") && value.trim() !== "" && Number.isFinite(Number(value))) {
    return value;
  }
  if (type === "boolean") {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "false")
      return lower;
  }
  return sqlLiteral(value);
}
function likeEscape(pattern) {
  return pattern.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
}
function membershipExpr(dimRef, filter, type) {
  const negate = filter.mode === "exclude";
  const hasNull = filter.values.includes(NULL_TOKEN);
  const present = filter.values.filter((value) => value !== NULL_TOKEN);
  let presentExpr = null;
  if (present.length === 1) {
    presentExpr = `${dimRef} ${negate ? "!=" : "="} ${filterLiteral(present[0], type)}`;
  } else if (present.length > 1) {
    const list = present.map((v3) => filterLiteral(v3, type)).join(", ");
    presentExpr = `${dimRef} ${negate ? "NOT IN" : "IN"} (${list})`;
  }
  if (!negate) {
    const parts = [];
    if (presentExpr)
      parts.push(presentExpr);
    if (hasNull)
      parts.push(`${dimRef} IS NULL`);
    if (parts.length === 0)
      return null;
    return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
  }
  if (hasNull) {
    const parts = [];
    if (presentExpr)
      parts.push(presentExpr);
    parts.push(`${dimRef} IS NOT NULL`);
    return parts.length === 1 ? parts[0] : `(${parts.join(" AND ")})`;
  }
  if (!presentExpr)
    return null;
  return `(${presentExpr} OR ${dimRef} IS NULL)`;
}
function filterExprs(filters, opts = {}) {
  const out = [];
  for (const [dimRef, filter] of Object.entries(filters)) {
    if (dimRef === opts.excludeDim || isEmptyFilter(filter))
      continue;
    const type = opts.types?.[dimRef];
    if (filter.mode === "contains") {
      const pat = sqlLiteral(`%${likeEscape(filter.pattern ?? "")}%`);
      out.push(`CAST(${dimRef} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
      continue;
    }
    const expr = membershipExpr(dimRef, filter, type);
    if (expr)
      out.push(expr);
  }
  return out;
}
function composeFilters(filters, opts = {}) {
  const base = filterExprs(filters, { types: opts.types, excludeDim: opts.excludeDim });
  if (opts.timeRef && opts.range)
    base.push(...timeFilters(opts.timeRef, opts.range));
  return base;
}
function distinctValues(dimRef, filters, limit = 50) {
  return { dimensions: [dimRef], filters, orderBy: [`${dimRef} ASC`], limit };
}

// webapp/src/state/ExplorerContext.tsx
var import_react2 = __toESM(require_react(), 1);

// webapp/src/state/url.ts
var GRAINS = new Set(ALL_GRAINS);
var CONTEXT_COLUMNS = new Set(["none", "pctTotal", "delta", "deltaPct"]);
var COMPARISONS = new Set(["off", "previous", "year", "custom"]);
var FILTER_MODES = new Set(["include", "exclude", "contains"]);

// webapp/src/state/ExplorerContext.tsx
var jsx_runtime2 = __toESM(require_jsx_runtime(), 1);
var ExplorerContext = import_react2.createContext(null);
function useExplorer() {
  const value = import_react2.useContext(ExplorerContext);
  if (!value)
    throw new Error("useExplorer must be used within ExplorerProvider");
  return value;
}

// webapp/src/state/useQueryResult.ts
var import_react4 = __toESM(require_react(), 1);

// webapp/src/state/queryActivity.ts
var import_react3 = __toESM(require_react(), 1);
var store = { active: 0, listeners: new Set };
function emit() {
  for (const listener of store.listeners)
    listener();
}
function beginQuery() {
  store.active += 1;
  emit();
}
function endQuery() {
  store.active = Math.max(0, store.active - 1);
  emit();
}

// webapp/src/state/useQueryResult.ts
var DEBOUNCE_MS = 80;
function useQueryResult(backend, query) {
  const [state, setState] = import_react4.useState({ loading: false });
  const token = import_react4.useRef(0);
  const key = query ? JSON.stringify(query) : null;
  import_react4.useEffect(() => {
    if (!query) {
      setState({ loading: false });
      return;
    }
    const current = ++token.current;
    setState((prev) => ({ result: prev.result, loading: true }));
    const timer = setTimeout(() => {
      beginQuery();
      backend.runQuery(query).then((result) => {
        if (current === token.current)
          setState({ result, loading: false });
      }).catch((err) => {
        if (current === token.current) {
          setState({ loading: false, error: err instanceof Error ? err.message : String(err) });
        }
      }).finally(() => endQuery());
    }, DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [key, backend]);
  return state;
}

// webapp/src/components/FilterEditor.tsx
var jsx_runtime3 = __toESM(require_jsx_runtime(), 1);
var MODES = [
  { mode: "include", label: "Include" },
  { mode: "exclude", label: "Exclude" },
  { mode: "contains", label: "Contains" }
];
var VALUE_LIMIT = 50;
var SEARCH_DEBOUNCE_MS = 200;
function useDebounced(value, delayMs) {
  const [debounced, setDebounced] = import_react5.useState(value);
  import_react5.useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}
function FilterEditor({
  dim,
  model,
  onClose
}) {
  const { state, dispatch, backend } = useExplorer();
  const filter = state.filters[dim.ref];
  const [mode, setModeState] = import_react5.useState(filter?.mode ?? "include");
  const selected = import_react5.useMemo(() => new Set(filter?.mode !== "contains" ? filter?.values ?? [] : []), [filter]);
  const panelRef = import_react5.useRef(null);
  const searchRef = import_react5.useRef(null);
  const labelId = import_react5.useId();
  const [search, setSearch] = import_react5.useState("");
  const debouncedSearch = useDebounced(search, SEARCH_DEBOUNCE_MS);
  const pattern = filter?.mode === "contains" ? filter.pattern ?? "" : "";
  const [patternDraft, setPatternDraft] = import_react5.useState(pattern);
  const debouncedPattern = useDebounced(patternDraft, SEARCH_DEBOUNCE_MS);
  import_react5.useEffect(() => {
    const opener = document.activeElement;
    searchRef.current?.focus();
    return () => opener?.focus?.();
  }, []);
  import_react5.useEffect(() => {
    function onKey(event) {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
      }
    }
    function onPointer(event) {
      if (panelRef.current && !panelRef.current.contains(event.target))
        onClose();
    }
    document.addEventListener("keydown", onKey, true);
    document.addEventListener("mousedown", onPointer, true);
    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.removeEventListener("mousedown", onPointer, true);
    };
  }, [onClose]);
  import_react5.useEffect(() => {
    if (mode !== "contains")
      return;
    if (debouncedPattern === pattern)
      return;
    dispatch({ type: "setFilterPattern", dim: dim.ref, pattern: debouncedPattern });
  }, [debouncedPattern, mode, dim.ref, dispatch]);
  const timeRef = model.timeDimension?.ref;
  const valueFilters = import_react5.useMemo(() => {
    const base = composeFilters(state.filters, { timeRef, range: state.dateRange, excludeDim: dim.ref });
    if (debouncedSearch.trim()) {
      const pat = sqlLiteral(`%${likeEscape(debouncedSearch.trim())}%`);
      base.push(`CAST(${dim.ref} AS VARCHAR) ILIKE ${pat} ESCAPE '\\'`);
    }
    return base;
  }, [state.filters, timeRef, state.dateRange, dim.ref, debouncedSearch]);
  const listMode = mode !== "contains";
  const { result, loading, error } = useQueryResult(backend, listMode ? distinctValues(dim.ref, valueFilters, VALUE_LIMIT) : null);
  const dimAlias = aliasOf(dim.ref);
  const values = import_react5.useMemo(() => {
    if (!result)
      return [];
    return result.rows.map((row) => {
      const raw = row[dimAlias];
      return raw === null || raw === undefined ? NULL_TOKEN : String(raw);
    });
  }, [result, dimAlias]);
  const stale = !!result && result.rows.length > 0 && !result.columns.includes(dimAlias);
  const showSkeleton = listMode && (loading || stale);
  function setMode(next) {
    setModeState(next);
    if (filter)
      dispatch({ type: "setFilterMode", dim: dim.ref, mode: next });
    if (next === "contains")
      setPatternDraft(pattern);
  }
  function onKeyDown(event) {
    if (event.key !== "Tab")
      return;
    const focusable = panelRef.current?.querySelectorAll('button, input, [href], select, textarea, [tabindex]:not([tabindex="-1"])');
    if (!focusable || focusable.length === 0)
      return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }
  return /* @__PURE__ */ jsx_runtime3.jsxs("div", {
    ref: panelRef,
    role: "dialog",
    "aria-modal": "true",
    "aria-labelledby": labelId,
    onKeyDown,
    className: "absolute left-0 z-50 mt-1 w-64 border border-line bg-surface p-2 text-2xs shadow-lg",
    children: [
      /* @__PURE__ */ jsx_runtime3.jsxs("div", {
        id: labelId,
        className: "mb-2 flex items-baseline justify-between gap-2",
        children: [
          /* @__PURE__ */ jsx_runtime3.jsx("span", {
            className: "truncate font-semibold text-ink",
            children: dim.label
          }),
          /* @__PURE__ */ jsx_runtime3.jsx("button", {
            type: "button",
            "aria-label": "Close filter editor",
            onClick: onClose,
            className: "grid size-4 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
            children: "×"
          })
        ]
      }),
      /* @__PURE__ */ jsx_runtime3.jsx("div", {
        role: "group",
        "aria-label": "Filter mode",
        className: "mb-2 grid grid-cols-3 gap-px border border-line bg-line",
        children: MODES.map(({ mode: m2, label }) => /* @__PURE__ */ jsx_runtime3.jsx("button", {
          type: "button",
          "aria-pressed": mode === m2,
          onClick: () => setMode(m2),
          className: `px-1.5 py-1 text-center ${mode === m2 ? "bg-accent-soft font-medium text-accent" : "bg-surface text-muted hover:bg-surface-soft"}`,
          children: label
        }, m2))
      }),
      mode === "contains" ? /* @__PURE__ */ jsx_runtime3.jsx("input", {
        ref: searchRef,
        type: "text",
        "aria-label": `${dim.label} contains`,
        placeholder: "Substring…",
        value: patternDraft,
        onChange: (event) => setPatternDraft(event.target.value),
        className: "w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
      }) : /* @__PURE__ */ jsx_runtime3.jsxs(jsx_runtime3.Fragment, {
        children: [
          /* @__PURE__ */ jsx_runtime3.jsx("input", {
            ref: searchRef,
            type: "text",
            "aria-label": `Search ${dim.label} values`,
            placeholder: "Search values…",
            value: search,
            onChange: (event) => setSearch(event.target.value),
            className: "w-full border border-line bg-surface px-1.5 py-1 text-2xs text-ink placeholder:text-faint"
          }),
          /* @__PURE__ */ jsx_runtime3.jsx("div", {
            className: "mt-2 max-h-56 overflow-y-auto",
            role: "group",
            "aria-label": `${dim.label} values`,
            children: error ? /* @__PURE__ */ jsx_runtime3.jsx("p", {
              className: "px-1 py-2 text-danger",
              children: error
            }) : showSkeleton ? /* @__PURE__ */ jsx_runtime3.jsx("div", {
              className: "space-y-1.5 p-1",
              children: [0, 1, 2, 3, 4].map((i) => /* @__PURE__ */ jsx_runtime3.jsx("div", {
                className: "skeleton h-4 w-full"
              }, i))
            }) : values.length === 0 ? /* @__PURE__ */ jsx_runtime3.jsx("p", {
              className: "px-1 py-2 text-faint",
              children: "No values"
            }) : values.map((value) => {
              const checked = selected.has(value);
              return /* @__PURE__ */ jsx_runtime3.jsxs("label", {
                className: "flex cursor-pointer items-center gap-2 px-1 py-1 hover:bg-surface-soft",
                children: [
                  /* @__PURE__ */ jsx_runtime3.jsx("input", {
                    type: "checkbox",
                    checked,
                    onChange: () => dispatch({ type: "toggleFilter", dim: dim.ref, value, mode }),
                    className: "size-3 accent-[var(--accent)]"
                  }),
                  /* @__PURE__ */ jsx_runtime3.jsx("span", {
                    className: "min-w-0 truncate text-ink",
                    children: displayDimValue(value)
                  })
                ]
              }, value);
            })
          })
        ]
      }),
      /* @__PURE__ */ jsx_runtime3.jsxs("div", {
        className: "mt-2 flex items-center justify-between border-t border-line pt-2",
        children: [
          /* @__PURE__ */ jsx_runtime3.jsx("button", {
            type: "button",
            onClick: () => dispatch({ type: "removeFilterDim", dim: dim.ref }),
            className: "text-muted underline-offset-2 hover:text-ink hover:underline",
            children: "Clear"
          }),
          /* @__PURE__ */ jsx_runtime3.jsx("button", {
            type: "button",
            onClick: onClose,
            className: "border border-line px-2 py-1 text-muted hover:bg-surface-soft",
            children: "Done"
          })
        ]
      })
    ]
  });
}

// webapp/src/components/FilterPill.tsx
var jsx_runtime4 = __toESM(require_jsx_runtime(), 1);
function FilterPill(props) {
  const [open, setOpen] = import_react6.useState(false);
  if (!("dim" in props)) {
    return /* @__PURE__ */ jsx_runtime4.jsxs("span", {
      "data-dimension": props.dimension,
      "data-value": props.value,
      className: "inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted",
      children: [
        /* @__PURE__ */ jsx_runtime4.jsxs("span", {
          className: "truncate",
          children: [
            /* @__PURE__ */ jsx_runtime4.jsxs("span", {
              className: "text-faint",
              children: [
                props.dimensionLabel ?? props.dimension,
                ":"
              ]
            }),
            " ",
            props.value
          ]
        }),
        props.onRemove ? /* @__PURE__ */ jsx_runtime4.jsx("button", {
          type: "button",
          "aria-label": `Remove filter ${props.value}`,
          onClick: props.onRemove,
          className: "grid size-3.5 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
          children: "×"
        }) : null
      ]
    });
  }
  const { dim, model, filter, onRemove } = props;
  return /* @__PURE__ */ jsx_runtime4.jsxs("span", {
    className: "relative inline-flex max-w-full items-center",
    "data-dimension": dim.ref,
    "data-mode": filter.mode,
    children: [
      /* @__PURE__ */ jsx_runtime4.jsxs("span", {
        className: "inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted",
        children: [
          /* @__PURE__ */ jsx_runtime4.jsxs("button", {
            type: "button",
            "aria-label": `Edit filter ${dim.label}`,
            "aria-haspopup": "dialog",
            "aria-expanded": open,
            onClick: () => setOpen((v3) => !v3),
            className: "min-w-0 truncate text-left hover:text-ink",
            children: [
              /* @__PURE__ */ jsx_runtime4.jsx("span", {
                className: "text-faint",
                children: dim.label
              }),
              " ",
              filterSummary(filter)
            ]
          }),
          /* @__PURE__ */ jsx_runtime4.jsx("button", {
            type: "button",
            "aria-label": `Remove filter ${dim.label}`,
            onClick: onRemove,
            className: "grid size-3.5 shrink-0 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink",
            children: "×"
          })
        ]
      }),
      open ? /* @__PURE__ */ jsx_runtime4.jsx(FilterEditor, {
        dim,
        model,
        onClose: () => setOpen(false)
      }) : null
    ]
  });
}

// webapp/src/components/Leaderboard.tsx
var jsx_runtime5 = __toESM(require_jsx_runtime(), 1);
var CONTEXT_TONE = {
  positive: "text-accent",
  negative: "text-danger",
  neutral: "text-faint"
};
function Leaderboard({
  dimension,
  title,
  metricLabel,
  rows,
  selectedValues = [],
  loading,
  formatMetric,
  onToggle,
  contextColumn = "none",
  contextOptions,
  onContextColumn,
  collapsedLimit = 6,
  expanded = false,
  onExpandedChange
}) {
  const selected = new Set(selectedValues);
  const visibleRows = expanded ? rows : rows.slice(0, collapsedLimit);
  const maxMagnitude = Math.max(1, ...visibleRows.map((row) => Math.abs(row.metric)));
  const expandable = expanded || rows.length > collapsedLimit;
  const showContext = contextColumn !== "none";
  const rowGrid = showContext ? "grid-cols-[minmax(0,1fr)_auto_auto]" : "grid-cols-[minmax(0,1fr)_auto]";
  return /* @__PURE__ */ jsx_runtime5.jsxs("section", {
    "data-testid": "dimension-leaderboard",
    "data-dimension": dimension,
    "data-expanded": expanded || undefined,
    "aria-label": `${title}, ranked by ${metricLabel}`,
    className: "flex min-h-60 flex-col border-b border-r border-line bg-surface data-[expanded=true]:col-span-full",
    children: [
      /* @__PURE__ */ jsx_runtime5.jsxs("header", {
        className: "flex items-center justify-between gap-3 px-3 pb-2 pt-2.5",
        children: [
          /* @__PURE__ */ jsx_runtime5.jsxs("div", {
            className: "flex min-w-0 items-baseline gap-2",
            children: [
              /* @__PURE__ */ jsx_runtime5.jsx("h3", {
                className: "truncate text-sm font-semibold text-ink",
                children: title
              }),
              /* @__PURE__ */ jsx_runtime5.jsxs("p", {
                className: "sr-only",
                children: [
                  "Ranked by ",
                  metricLabel
                ]
              })
            ]
          }),
          contextOptions && onContextColumn ? /* @__PURE__ */ jsx_runtime5.jsx("div", {
            role: "group",
            "aria-label": "Context column",
            "data-testid": "leaderboard-context-toggle",
            className: "flex shrink-0 overflow-hidden border border-line text-2xs",
            children: contextOptions.map((option) => /* @__PURE__ */ jsx_runtime5.jsx("button", {
              type: "button",
              title: option.title,
              "aria-pressed": contextColumn === option.key,
              "data-context": option.key,
              "data-active": contextColumn === option.key || undefined,
              onClick: () => onContextColumn(option.key),
              className: "border-l border-line px-1.5 py-0.5 font-mono text-faint first:border-l-0 hover:bg-surface-soft data-[active=true]:bg-accent-soft data-[active=true]:text-accent",
              children: option.label
            }, option.key))
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx_runtime5.jsx("div", {
        "data-testid": "leaderboard-rows",
        children: loading && rows.length === 0 ? /* @__PURE__ */ jsx_runtime5.jsx("div", {
          className: "space-y-2 p-3",
          children: [0, 1, 2, 3].map((i) => /* @__PURE__ */ jsx_runtime5.jsx("div", {
            className: "skeleton h-5 w-full"
          }, i))
        }) : rows.length === 0 ? /* @__PURE__ */ jsx_runtime5.jsx("p", {
          className: "px-3 py-4 text-xs text-faint",
          children: "No values"
        }) : visibleRows.map((row) => {
          const tone = row.metric < 0 ? "negative" : "positive";
          const isSelected = selected.has(row.value);
          const width = `${Math.round(Math.abs(row.metric) / maxMagnitude * 100)}%`;
          return /* @__PURE__ */ jsx_runtime5.jsxs("button", {
            type: "button",
            "data-dimension": dimension,
            "data-value": row.value,
            "data-selected": isSelected || undefined,
            "data-tone": tone,
            onClick: () => onToggle?.(row.value),
            "aria-pressed": isSelected,
            className: `leaderboard-row relative grid w-full ${rowGrid} items-center gap-3 overflow-hidden border-0 bg-transparent px-3 py-1 text-left text-xs text-ink data-[selected=true]:bg-chart-primary-selected`,
            children: [
              /* @__PURE__ */ jsx_runtime5.jsx("span", {
                "aria-hidden": "true",
                className: `absolute inset-y-0 left-0 ${tone === "negative" ? "bg-danger-soft" : "bg-chart-primary-soft"}`,
                style: { width }
              }),
              /* @__PURE__ */ jsx_runtime5.jsx("span", {
                className: "relative min-w-0 truncate text-muted",
                children: displayDimValue(row.value)
              }),
              /* @__PURE__ */ jsx_runtime5.jsx("strong", {
                className: "relative tnum font-semibold text-ink",
                children: formatMetric(row.metric)
              }),
              showContext ? /* @__PURE__ */ jsx_runtime5.jsx("span", {
                "data-testid": "leaderboard-context",
                "data-tone": row.context?.tone ?? "neutral",
                className: `relative w-14 text-right font-mono tnum text-2xs ${CONTEXT_TONE[row.context?.tone ?? "neutral"]}`,
                children: row.context?.label ?? "—"
              }) : null
            ]
          }, `${dimension}:${row.value}`);
        })
      }),
      expandable && !loading ? /* @__PURE__ */ jsx_runtime5.jsx("button", {
        type: "button",
        "data-action": expanded ? "leaderboard-back" : "leaderboard-expand",
        "aria-expanded": expanded,
        onClick: () => onExpandedChange?.(!expanded),
        className: "leaderboard-expand mt-1 min-h-9 border-0 border-t border-line bg-transparent px-3 text-left text-xs font-normal text-faint hover:text-accent",
        children: expanded ? "← All dimensions" : `Expand table (${rows.length})`
      }) : null
    ]
  });
}

// webapp/src/components/MetricCard.tsx
var import_react9 = __toESM(require_react(), 1);

// webapp/src/components/Sparkline.tsx
var import_react8 = __toESM(require_react(), 1);

// webapp/src/components/ChartTooltip.tsx
var import_react7 = __toESM(require_react(), 1);
var jsx_runtime6 = __toESM(require_jsx_runtime(), 1);
function ChartTooltip({
  tip,
  position = "fixed",
  offset = 12,
  className,
  style
}) {
  if (!tip)
    return null;
  return /* @__PURE__ */ jsx_runtime6.jsx("div", {
    role: "tooltip",
    style: { position, left: tip.x + offset, top: tip.y + offset, pointerEvents: "none", zIndex: 50, ...style },
    className: className || "rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white shadow",
    children: tip.content
  });
}

// webapp/src/components/Sparkline.tsx
var jsx_runtime7 = __toESM(require_jsx_runtime(), 1);
function Sparkline({
  values,
  labels,
  height = 44,
  ariaLabel,
  formatValue: formatValue2 = (value) => value.toLocaleString(undefined, { maximumFractionDigits: 2 }),
  onHover,
  onBrush
}) {
  const containerRef = import_react8.useRef(null);
  const svgRef = import_react8.useRef(null);
  const dragStart = import_react8.useRef(null);
  const [width, setWidth] = import_react8.useState(200);
  const [hover, setHover] = import_react8.useState(null);
  const [brush, setBrush] = import_react8.useState(null);
  const [tip, setTip] = import_react8.useState(null);
  import_react8.useEffect(() => {
    const node = containerRef.current;
    if (!node || typeof ResizeObserver === "undefined")
      return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries)
        setWidth(Math.max(40, entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  const points = values.map((value, index) => ({ index, value })).filter((point) => Number.isFinite(point.value));
  if (points.length < 2) {
    return /* @__PURE__ */ jsx_runtime7.jsx("svg", {
      ref: svgRef,
      role: "img",
      "aria-label": ariaLabel || "No trend data",
      className: "h-11 w-full",
      viewBox: `0 0 ${width} ${height}`
    });
  }
  const pad = 3;
  const min = Math.min(...points.map((point) => point.value));
  const max = Math.max(...points.map((point) => point.value));
  const span = max - min || 1;
  const coordinates = points.map((point, index) => ({
    ...point,
    x: pad + index / (points.length - 1) * (width - pad * 2),
    y: pad + (1 - (point.value - min) / span) * (height - pad * 2)
  }));
  const line = coordinates.map(({ x: x2, y: y2 }) => `${x2.toFixed(1)},${y2.toFixed(1)}`).join(" L ");
  const area = `M ${coordinates[0].x.toFixed(1)},${height - pad} L ${line} L ${coordinates.at(-1).x.toFixed(1)},${height - pad} Z`;
  const latest = coordinates.at(-1);
  const summary = ariaLabel || `Trend of ${points.length} points, latest ${formatValue2(latest.value)}`;
  function localX(event) {
    const rect = svgRef.current?.getBoundingClientRect();
    return rect ? Math.max(pad, Math.min(width - pad, (event.clientX - rect.left) / rect.width * width)) : pad;
  }
  function indexAt(x2) {
    return Math.max(0, Math.min(coordinates.length - 1, Math.round((x2 - pad) / (width - pad * 2) * (coordinates.length - 1))));
  }
  function move(event) {
    const x2 = localX(event);
    const index = indexAt(x2);
    const point = coordinates[index];
    setHover(index);
    setTip({
      content: `${labels?.[point.index] ? `${labels[point.index]}: ` : ""}${formatValue2(point.value)}`,
      x: event.clientX,
      y: event.clientY
    });
    onHover?.({ index: point.index, label: labels?.[point.index], value: point.value });
    if (dragStart.current !== null)
      setBrush({ a: dragStart.current, b: x2 });
  }
  function down(event) {
    if (!onBrush || !labels?.length)
      return;
    event.currentTarget.setPointerCapture(event.pointerId);
    const x2 = localX(event);
    dragStart.current = x2;
    setBrush({ a: x2, b: x2 });
  }
  function up(event) {
    if (dragStart.current === null || !onBrush || !labels?.length)
      return;
    if (event.currentTarget.hasPointerCapture(event.pointerId))
      event.currentTarget.releasePointerCapture(event.pointerId);
    const end = localX(event);
    if (Math.abs(end - dragStart.current) > 6) {
      const startPoint = coordinates[indexAt(Math.min(dragStart.current, end))];
      const endPoint = coordinates[indexAt(Math.max(dragStart.current, end))];
      onBrush({ from: labels[startPoint.index], to: labels[endPoint.index] });
    }
    dragStart.current = null;
    setBrush(null);
  }
  function leave() {
    setHover(null);
    setTip(null);
    onHover?.(null);
    if (dragStart.current === null)
      setBrush(null);
  }
  const hovered = hover === null ? null : coordinates[hover];
  return /* @__PURE__ */ jsx_runtime7.jsxs("span", {
    ref: containerRef,
    className: "relative block w-full",
    children: [
      /* @__PURE__ */ jsx_runtime7.jsxs("svg", {
        ref: svgRef,
        role: "img",
        "aria-label": summary,
        className: `h-11 w-full overflow-hidden text-chart-primary ${onBrush ? "touch-none select-none" : ""}`,
        viewBox: `0 0 ${width} ${height}`,
        preserveAspectRatio: "none",
        onPointerMove: move,
        onPointerDown: down,
        onPointerUp: up,
        onPointerCancel: leave,
        onPointerLeave: leave,
        onDoubleClick: () => onBrush?.(null),
        children: [
          /* @__PURE__ */ jsx_runtime7.jsx("path", {
            d: area,
            fill: "currentColor",
            opacity: 0.1
          }),
          /* @__PURE__ */ jsx_runtime7.jsx("path", {
            d: `M ${line}`,
            fill: "none",
            stroke: "currentColor",
            strokeWidth: 1.5,
            vectorEffect: "non-scaling-stroke"
          }),
          brush ? /* @__PURE__ */ jsx_runtime7.jsx("rect", {
            x: Math.min(brush.a, brush.b),
            y: 0,
            width: Math.abs(brush.b - brush.a),
            height,
            fill: "currentColor",
            opacity: 0.12
          }) : null,
          hovered ? /* @__PURE__ */ jsx_runtime7.jsxs(jsx_runtime7.Fragment, {
            children: [
              /* @__PURE__ */ jsx_runtime7.jsx("line", {
                x1: hovered.x,
                x2: hovered.x,
                y1: 0,
                y2: height,
                stroke: "currentColor",
                strokeWidth: 1,
                opacity: 0.45
              }),
              /* @__PURE__ */ jsx_runtime7.jsx("circle", {
                cx: hovered.x,
                cy: hovered.y,
                r: 2.5,
                fill: "currentColor"
              })
            ]
          }) : /* @__PURE__ */ jsx_runtime7.jsx("circle", {
            cx: latest.x,
            cy: latest.y,
            r: 2.25,
            fill: "currentColor"
          })
        ]
      }),
      /* @__PURE__ */ jsx_runtime7.jsx(ChartTooltip, {
        tip
      })
    ]
  });
}

// webapp/src/components/MetricCard.tsx
var jsx_runtime8 = __toESM(require_jsx_runtime(), 1);
var TONE_CLASS = {
  positive: "text-accent",
  negative: "text-danger",
  neutral: "text-faint"
};
var TONE_ARROW = { positive: "▲", negative: "▼", neutral: "·" };
function MetricCard({
  metric,
  label,
  value,
  valueText,
  format,
  delta,
  sparkValues = [],
  sparkLabels,
  selected,
  loading,
  onSelect,
  onSparkHover,
  onSparkBrush
}) {
  const [sparkHover, setSparkHover] = import_react9.useState(null);
  const summary = /* @__PURE__ */ jsx_runtime8.jsxs(jsx_runtime8.Fragment, {
    children: [
      /* @__PURE__ */ jsx_runtime8.jsxs("div", {
        className: "flex items-baseline justify-between gap-2",
        children: [
          /* @__PURE__ */ jsx_runtime8.jsx("span", {
            className: "truncate text-2xs font-semibold uppercase tracking-wide text-faint",
            children: label
          }),
          sparkHover?.label ? /* @__PURE__ */ jsx_runtime8.jsx("span", {
            className: "shrink-0 font-mono text-2xs text-faint",
            children: sparkHover.label
          }) : delta ? /* @__PURE__ */ jsx_runtime8.jsxs("span", {
            "data-tone": delta.tone,
            className: `shrink-0 text-2xs font-medium ${TONE_CLASS[delta.tone]}`,
            children: [
              /* @__PURE__ */ jsx_runtime8.jsx("span", {
                "aria-hidden": "true",
                className: "mr-0.5 text-[8px]",
                children: TONE_ARROW[delta.tone]
              }),
              delta.label
            ]
          }) : null
        ]
      }),
      /* @__PURE__ */ jsx_runtime8.jsx("div", {
        className: "font-mono tnum text-base font-semibold text-ink",
        children: loading ? /* @__PURE__ */ jsx_runtime8.jsx("span", {
          className: "skeleton inline-block h-5 w-24 align-middle"
        }) : sparkHover ? formatValue(sparkHover.value, format) : valueText ?? formatValue(value, format)
      })
    ]
  });
  const className = "group flex w-full flex-col gap-1.5 border border-line bg-surface px-3 py-2.5 text-left data-[selected=true]:border-accent data-[selected=true]:ring-1 data-[selected=true]:ring-accent";
  const sparkline = /* @__PURE__ */ jsx_runtime8.jsx(Sparkline, {
    values: sparkValues,
    labels: sparkLabels,
    onHover: (point) => {
      setSparkHover(point);
      onSparkHover?.(point);
    },
    onBrush: onSparkBrush,
    formatValue: (sparkValue) => formatValue(sparkValue, format)
  });
  if (!onSelect) {
    return /* @__PURE__ */ jsx_runtime8.jsxs("article", {
      "data-metric": metric,
      "data-selected": selected || undefined,
      className,
      children: [
        summary,
        sparkline
      ]
    });
  }
  return /* @__PURE__ */ jsx_runtime8.jsxs("article", {
    "data-metric": metric,
    "data-selected": selected || undefined,
    className,
    children: [
      /* @__PURE__ */ jsx_runtime8.jsx("button", {
        type: "button",
        "data-metric": metric,
        "aria-pressed": !!selected,
        onClick: () => onSelect(metric),
        className: "-m-1 flex flex-col gap-1 border-0 bg-transparent p-1 text-left transition hover:opacity-75",
        children: summary
      }),
      sparkline
    ]
  });
}

// webapp/src/components/QueryDebugPanel.tsx
var jsx_runtime9 = __toESM(require_jsx_runtime(), 1);
var SQL_KEYWORDS = new Set([
  "and",
  "as",
  "asc",
  "by",
  "case",
  "cast",
  "count",
  "date_trunc",
  "desc",
  "else",
  "end",
  "from",
  "group",
  "in",
  "is",
  "join",
  "left",
  "limit",
  "not",
  "null",
  "on",
  "or",
  "order",
  "over",
  "partition",
  "select",
  "sum",
  "then",
  "when",
  "where",
  "with"
]);
function tokenizeSql(source) {
  const tokens = [];
  let index = 0;
  while (index < source.length) {
    const rest = source.slice(index);
    const comment = rest.match(/^--[^\n]*/);
    const string = rest.match(/^'(?:''|[^'])*'/);
    const number = rest.match(/^\b\d+(?:\.\d+)?\b/);
    const word = rest.match(/^[A-Za-z_][A-Za-z0-9_]*/);
    const match = comment ?? string ?? number ?? word;
    if (!match) {
      tokens.push({ kind: "plain", value: source[index] });
      index += 1;
      continue;
    }
    const value = match[0];
    const kind = comment ? "comment" : string ? "string" : number ? "number" : SQL_KEYWORDS.has(value.toLowerCase()) ? "keyword" : "plain";
    tokens.push({ kind, value });
    index += value.length;
  }
  return tokens;
}
var TOKEN_CLASS = {
  comment: "italic text-faint",
  string: "text-accent",
  number: "text-danger",
  keyword: "font-semibold text-accent",
  plain: ""
};
function QueryDebugPanel({ queries, inputs = {} }) {
  const text = Object.entries(queries).filter(([, sql]) => sql).map(([name, sql]) => `-- ${name}
${sql}`).join(`

`);
  const tokens = tokenizeSql(text || "No queries yet.");
  return /* @__PURE__ */ jsx_runtime9.jsxs("details", {
    className: "border border-line bg-surface",
    children: [
      /* @__PURE__ */ jsx_runtime9.jsx("summary", {
        className: "cursor-pointer px-3 py-2 text-2xs font-semibold uppercase tracking-wide text-faint",
        children: "Generated SQL"
      }),
      Object.keys(inputs).length > 0 ? /* @__PURE__ */ jsx_runtime9.jsx("div", {
        "data-testid": "query-inputs",
        className: "grid gap-px border-t border-line bg-line sm:grid-cols-2",
        children: Object.entries(inputs).map(([name, input]) => input ? /* @__PURE__ */ jsx_runtime9.jsxs("section", {
          className: "min-w-0 bg-surface px-3 py-2 text-2xs",
          children: [
            /* @__PURE__ */ jsx_runtime9.jsx("h3", {
              className: "mb-1 font-semibold text-ink",
              children: name
            }),
            [
              ["Metrics", input.metrics],
              ["Dimensions", input.dimensions],
              ["Filters", input.filters]
            ].map(([label, values]) => values?.length ? /* @__PURE__ */ jsx_runtime9.jsxs("p", {
              className: "truncate text-muted",
              title: values.join(", "),
              children: [
                /* @__PURE__ */ jsx_runtime9.jsxs("strong", {
                  className: "font-medium text-faint",
                  children: [
                    label,
                    ":"
                  ]
                }),
                " ",
                values.join(", ")
              ]
            }, label) : null)
          ]
        }, name) : null)
      }) : null,
      /* @__PURE__ */ jsx_runtime9.jsx("pre", {
        "data-testid": "query-debug",
        className: "max-h-72 overflow-auto whitespace-pre-wrap border-t border-line px-3 py-2 font-mono text-2xs text-muted",
        children: tokens.map((token, index) => TOKEN_CLASS[token.kind] ? /* @__PURE__ */ jsx_runtime9.jsx("span", {
          className: TOKEN_CLASS[token.kind],
          "data-token": token.kind,
          children: token.value
        }, index) : token.value)
      })
    ]
  });
}

// webapp/src/components/States.tsx
var jsx_runtime10 = __toESM(require_jsx_runtime(), 1);
function StateBox({ tone, title, message }) {
  const danger = tone === "danger";
  return /* @__PURE__ */ jsx_runtime10.jsx("div", {
    className: `grid min-h-[200px] place-items-center border bg-surface p-6 text-center ${danger ? "border-danger/40" : "border-line"}`,
    "data-state": tone,
    role: danger ? "alert" : "status",
    "aria-live": danger ? "assertive" : "polite",
    children: /* @__PURE__ */ jsx_runtime10.jsxs("div", {
      className: "max-w-md",
      children: [
        tone === "loading" ? /* @__PURE__ */ jsx_runtime10.jsx("span", {
          "aria-hidden": "true",
          className: "motion-safe:animate-pulse inline-block size-2 rounded-full bg-accent"
        }) : null,
        title ? /* @__PURE__ */ jsx_runtime10.jsx("h3", {
          className: `text-sm font-semibold ${danger ? "text-danger" : "text-ink"}`,
          children: title
        }) : null,
        /* @__PURE__ */ jsx_runtime10.jsx("p", {
          className: `mt-1 text-xs ${danger ? "text-danger" : "text-muted"}`,
          children: message
        })
      ]
    })
  });
}
function LoadingState({ title = "Loading", message = "Loading metrics…" }) {
  return /* @__PURE__ */ jsx_runtime10.jsx(StateBox, {
    tone: "loading",
    title,
    message
  });
}
function EmptyState({ title = "No results", message }) {
  return /* @__PURE__ */ jsx_runtime10.jsx(StateBox, {
    tone: "muted",
    title,
    message
  });
}
function ErrorState({ title = "Query failed", message }) {
  return /* @__PURE__ */ jsx_runtime10.jsx(StateBox, {
    tone: "danger",
    title,
    message
  });
}
// webapp/src/lib/theme.ts
var TOKEN_PROPERTIES = {
  background: "--bg",
  surface: "--surface",
  surfaceSoft: "--surface-soft",
  ink: "--ink",
  muted: "--muted",
  faint: "--faint",
  line: "--line",
  action: "--accent",
  actionSoft: "--accent-soft",
  chartPrimary: "--chart-primary",
  chartPrimarySoft: "--chart-primary-soft",
  chartPrimarySelected: "--chart-primary-selected",
  danger: "--danger",
  dangerSoft: "--danger-soft"
};
var KEY = "sidemantic-theme";
function getTheme() {
  const stored = localStorage.getItem(KEY);
  if (stored === "light" || stored === "dark")
    return stored;
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
}
function toggleTheme() {
  const next = getTheme() === "dark" ? "light" : "dark";
  localStorage.setItem(KEY, next);
  applyTheme(next);
  return next;
}
function applyThemeTokens(tokens, target = document.documentElement) {
  for (const [name, value] of Object.entries(tokens)) {
    if (value)
      target.style.setProperty(TOKEN_PROPERTIES[name], value);
    else
      target.style.removeProperty(TOKEN_PROPERTIES[name]);
  }
}

// webapp/src/static-api.tsx
var roots = new WeakMap;
function mount(container, node) {
  let root = roots.get(container);
  if (!root) {
    container.replaceChildren();
    root = import_client.createRoot(container);
    roots.set(container, root);
  }
  import_react_dom.flushSync(() => root.render(node));
}
function rowsOf(query) {
  return query.result?.sample_rows ?? query.result?.rows ?? [];
}
function aliasFor(query, ref = "") {
  return query.output_aliases?.[ref] ?? aliasForSemanticRef(ref);
}
function resolveFormat(format, context) {
  return typeof format === "function" ? format(context) : format;
}
function metricConfigFor(metrics = [], metricKey) {
  return metrics.find((metric) => metric.key === metricKey) ?? metrics[0] ?? {};
}
function metricValueFormat(metrics = [], metricKey) {
  return metricConfigFor(metrics, metricKey).format === "currency" ? { currency: "USD", maximumFractionDigits: 0, style: "currency" } : { maximumFractionDigits: 0 };
}
function renderSelectOptions(select, options, selectedValue, config = {}) {
  select.replaceChildren(...options.map((option) => {
    const value = String(config.value?.(option) ?? option.key ?? option.value ?? option);
    const node = document.createElement("option");
    node.value = value;
    node.textContent = String(config.label?.(option) ?? option.label ?? value);
    node.selected = value === selectedValue;
    return node;
  }));
}
function syncScrollPosition(source, target) {
  target.scrollLeft = source.scrollLeft;
  target.scrollTop = source.scrollTop;
}
function filterZeroMetricRows(result, metricKey) {
  return { columns: result.columns ?? [], rows: (result.rows ?? result.sample_rows ?? []).filter((row) => Number(row[metricKey]) !== 0) };
}
function highlightCode(element, source) {
  element.textContent = source;
}
function toComponentResult(result = {}) {
  return { columns: result.columns ?? [], sample_rows: result.sample_rows ?? result.rows ?? [] };
}
function toComponentQuery({ dimensions = [], metrics = [], result, outputAliases } = {}) {
  return { dimensions, metrics, output_aliases: outputAliases, result: toComponentResult(result) };
}
function renderMetricCards(container, query, options = {}) {
  const row = rowsOf(query)[0] ?? {};
  mount(container, import_react10.createElement(import_react10.Fragment, null, ...(query.metrics ?? []).map((metric) => {
    const key = aliasFor(query, metric);
    const format = typeof options.valueFormat === "function" ? options.valueFormat({ metric, key, value: row[key] }) : options.valueFormat;
    return import_react10.createElement(MetricCard, {
      key: metric,
      metric,
      label: options.labels?.[metric] ?? aliasForSemanticRef(metric).replaceAll("_", " "),
      value: row[key],
      format,
      delta: options.deltas?.[metric],
      selected: options.selectedMetric === metric,
      onSelect: options.onSelect ? () => options.onSelect({ metric, key, value: row[key] }) : undefined
    });
  })));
}
function renderMetricSummaryCards(container, config = {}) {
  const totals = config.totals?.rows?.[0] ?? {};
  const series = config.seriesRows ?? [];
  mount(container, import_react10.createElement(import_react10.Fragment, null, ...(config.metrics ?? []).map((metric) => {
    const key = metric.key;
    const alias = aliasForSemanticRef(key);
    return import_react10.createElement(MetricCard, {
      key,
      metric: key,
      label: metric.label ?? alias.replaceAll("_", " "),
      value: totals[alias],
      format: resolveFormat(config.valueFormat, key),
      selected: config.selectedMetric === key,
      sparkValues: series.map((row) => Number(row[alias]) || 0),
      sparkLabels: series.map((row) => String(row[config.timeKey] ?? "")),
      onSparkBrush: config.onBrush ? (range) => config.onBrush(range?.from ?? null, range?.to ?? null) : undefined,
      onSelect: config.onSelect ? () => config.onSelect({ metric: key, key: alias, value: totals[alias] }) : undefined
    });
  })));
}
function renderLeaderboard(container, query, options = {}) {
  const dimension = query.dimensions?.[0] ?? "";
  const metric = options.metricRef ?? query.metrics?.[0] ?? "";
  const dimensionKey = aliasFor(query, dimension);
  const metricKey = aliasFor(query, metric);
  const allRows = rowsOf(query);
  const expanded = options.expanded ?? container.__sdmExpanded ?? false;
  mount(container, import_react10.createElement(Leaderboard, {
    dimension,
    title: options.dimensionLabel ?? dimensionKey.replaceAll("_", " "),
    metricLabel: options.metricLabel ?? metricKey.replaceAll("_", " "),
    rows: allRows.map((row) => ({ value: normalizeFilterValue(row[dimensionKey]), metric: Number(row[metricKey]) || 0 })),
    selectedValues: options.selectedValues ?? (options.selectedValue === undefined ? [] : [options.selectedValue]),
    collapsedLimit: options.limit || allRows.length,
    expanded,
    formatMetric: (value) => formatUiValue(value, typeof options.valueFormat === "function" ? options.valueFormat({ metric, key: metricKey, value }) : options.valueFormat),
    onToggle: options.onSelect ? (value) => options.onSelect({ dimension, value, row: allRows.find((row) => normalizeFilterValue(row[dimensionKey]) === value) }) : undefined,
    onExpandedChange: options.expandable ? (next) => {
      container.__sdmExpanded = next;
      options.onToggleExpand?.(next);
      renderLeaderboard(container, query, options);
    } : undefined
  }));
}
function renderDimensionLeaderboardCards(container, dimensions, config = {}) {
  const expandedDimension = container.__sdmExpandedDim;
  const visible = expandedDimension ? dimensions.filter((item) => (item.key ?? item) === expandedDimension) : dimensions;
  mount(container, import_react10.createElement(import_react10.Fragment, null, ...visible.map((item) => {
    const dimension = item.key ?? item;
    const result = config.resultForDimension?.(item) ?? { rows: [] };
    const query = toComponentQuery({ dimensions: [dimension], metrics: [config.metricRef], result });
    const rows = rowsOf(query);
    const expanded = expandedDimension === dimension;
    return import_react10.createElement(Leaderboard, {
      key: dimension,
      dimension,
      title: item.label ?? aliasForSemanticRef(dimension).replaceAll("_", " "),
      metricLabel: config.metricLabel?.(item) ?? config.metricName ?? aliasForSemanticRef(config.metricRef),
      rows: rows.map((row) => ({ value: normalizeFilterValue(row[aliasFor(query, dimension)]), metric: Number(row[aliasFor(query, config.metricRef)]) || 0 })),
      selectedValues: config.selectedValuesForDimension?.(item) ?? [],
      collapsedLimit: config.limit ?? 6,
      expanded,
      formatMetric: (value) => formatUiValue(value, resolveFormat(config.valueFormat, { metric: config.metricRef, value })),
      onToggle: config.onSelect ? (value) => config.onSelect({ dimension, value, row: rows.find((row) => normalizeFilterValue(row[aliasFor(query, dimension)]) === value) }) : undefined,
      onExpandedChange: config.expandable === false ? undefined : (next) => {
        container.__sdmExpandedDim = next ? dimension : undefined;
        renderDimensionLeaderboardCards(container, dimensions, config);
      }
    });
  })));
}
function renderFilterPills(container, filters, onRemove, options = {}) {
  const pills = Object.entries(filters ?? {}).flatMap(([dimension, values]) => (values ?? []).map((value) => {
    const normalized = normalizeFilterValue(value);
    return import_react10.createElement(FilterPill, { key: `${dimension}:${normalized}`, dimension, value: normalized, onRemove: onRemove ? () => onRemove({ dimension, value: normalized }) : undefined });
  }));
  for (const extra of options.extraPills ?? []) {
    pills.push(import_react10.createElement(FilterPill, {
      key: extra.key ?? `${extra.dimension}:${extra.value}`,
      dimension: extra.dimension,
      dimensionLabel: extra.dimensionLabel,
      value: extra.value,
      onRemove: extra.onRemove
    }));
  }
  mount(container, pills.length ? import_react10.createElement(import_react10.Fragment, null, ...pills) : options.emptyLabel ? import_react10.createElement("span", { className: "text-faint" }, options.emptyLabel) : null);
}
function renderHighlightedQueryDebug(container, queries) {
  mount(container, import_react10.createElement(QueryDebugPanel, { queries: Object.fromEntries(Object.entries(queries ?? {}).map(([name, query]) => [name, typeof query === "string" ? query : query?.sql])) }));
}
var renderQueryDebug = renderHighlightedQueryDebug;
function renderDataPreview(container, result, options = {}) {
  const columns = result?.columns ?? [];
  mount(container, import_react10.createElement(DataTable, {
    columns: columns.map((key) => ({ key, label: key.replaceAll("_", " "), numeric: (result.sample_rows ?? result.rows ?? []).some((row) => typeof row[key] === "number") })),
    rows: result.sample_rows ?? result.rows ?? [],
    pageSize: options.pageSize || 50,
    renderCell: (_column, value) => formatUiValue(value)
  }));
}
function renderState(container, state) {
  const Component = state.kind === "error" ? ErrorState : state.kind === "loading" ? LoadingState : EmptyState;
  mount(container, import_react10.createElement(Component, { message: state.message }));
}
function renderValidationState(stateElement, listElement, errors = []) {
  stateElement.textContent = errors.length ? "Invalid" : "Valid";
  stateElement.dataset.valid = String(errors.length === 0);
  listElement.replaceChildren(...errors.map((error) => Object.assign(document.createElement("li"), { textContent: error })));
}
function setControlsDisabled(selector, disabled) {
  document.querySelectorAll(selector).forEach((control) => {
    control.disabled = disabled;
  });
}
export {
  toggleTheme,
  toggleFilterValue,
  toComponentResult,
  toComponentQuery,
  syncScrollPosition,
  setControlsDisabled,
  renderValidationState,
  renderState,
  renderSelectOptions,
  renderQueryDebug,
  renderMetricSummaryCards,
  renderMetricCards,
  renderLeaderboard,
  renderHighlightedQueryDebug,
  renderFilterPills,
  renderDimensionLeaderboardCards,
  renderDataPreview,
  removeFilterValue,
  removeFilterDimension,
  normalizeFilterValue,
  metricValueFormat,
  metricConfigFor,
  labelize,
  highlightCode,
  getTheme,
  formatUiValue as formatValue,
  formatUiCompact as formatCompact,
  filterZeroMetricRows,
  applyThemeTokens,
  applyTheme,
  aliasForSemanticRef
};
