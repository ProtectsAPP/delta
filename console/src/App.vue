<template>
  <n-config-provider :theme="darkTheme" :theme-overrides="overrides">
    <n-message-provider>
      <n-dialog-provider>
        <n-notification-provider>
          <router-view />
        </n-notification-provider>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { NConfigProvider, NMessageProvider, NDialogProvider, NNotificationProvider, darkTheme } from 'naive-ui';
import type { GlobalThemeOverrides } from 'naive-ui';

// Theme that matches the public site: dark surfaces, mint accent.
const overrides: GlobalThemeOverrides = {
  common: {
    primaryColor:        '#4ade80',
    primaryColorHover:   '#5fe796',
    primaryColorPressed: '#22c55e',
    primaryColorSuppl:   '#4ade80',
    successColor:        '#4ade80',
    infoColor:           '#38bdf8',
    warningColor:        '#fbbf24',
    errorColor:          '#f87171',
    bodyColor:           '#0a0b0d',
    cardColor:           '#15181c',
    modalColor:          '#15181c',
    popoverColor:        '#1a1d22',
    tableColor:          '#15181c',
    inputColor:          '#1a1d22',
    actionColor:         '#1a1d22',
    tableHeaderColor:    '#1a1d22',
    hoverColor:          'rgba(74,222,128,0.08)',
    dividerColor:        '#20242a',
    borderColor:         '#20242a',
    textColorBase:       '#e7e9ec',
    textColor1:          '#e7e9ec',
    textColor2:          '#a8aeb8',
    textColor3:          '#6a7280',
    fontFamily:          "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    fontFamilyMono:      "'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace",
    borderRadius:        '8px',
    borderRadiusSmall:   '6px'
  },
  Card:        { color: '#15181c', borderColor: '#20242a' },
  Layout:      { color: '#0a0b0d', siderColor: '#0f1114', headerColor: '#0f1114' },
  Menu:        { itemColorHover: 'rgba(74,222,128,0.08)', itemTextColorActive: '#4ade80', itemIconColorActive: '#4ade80', itemColorActive: 'rgba(74,222,128,0.10)', itemColorActiveHover: 'rgba(74,222,128,0.14)' },
  Button:      { textColorPrimary: '#0a0b0d', textColorHoverPrimary: '#0a0b0d', textColorPressedPrimary: '#0a0b0d', textColorFocusPrimary: '#0a0b0d' },
  DataTable:   { thColor: '#1a1d22', tdColor: '#15181c', tdColorHover: '#1a1d22' },
  Tag:         { borderRadius: '4px' }
};
</script>
