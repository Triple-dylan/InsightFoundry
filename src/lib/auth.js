export function authContextFromHeaders(headers) {
  return {
    tenantId: headers["x-tenant-id"] ?? "",
    userId: headers["x-user-id"] ?? "system",
    role: headers["x-user-role"] ?? "analyst",
    channel: headers["x-channel-id"] ?? "web"
  };
}

export function requireRole(ctx, allowed) {
  if (!allowed.includes(ctx.role)) {
    const err = new Error(`Role '${ctx.role}' cannot perform this action`);
    err.statusCode = 403;
    throw err;
  }
}

export function requireTenantHeader(ctx) {
  if (!ctx.tenantId) {
    const err = new Error("Missing x-tenant-id header");
    err.statusCode = 400;
    throw err;
  }
}
