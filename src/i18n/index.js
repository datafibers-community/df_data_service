import { englishMessages } from 'admin-on-rest';
import frenchMessages from 'aor-language-french';
import chineseMessages from 'aor-language-chinese';

import customFrenchMessages from './fr';
import customEnglishMessages from './en';
import customChineseMessages from './ch';

export default {
    fr: { ...frenchMessages, ...customFrenchMessages },
    en: { ...englishMessages, ...customEnglishMessages },
    ch: { ...chineseMessages, ...customChineseMessages },
};
